package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/yandex-cloud/go-genproto/yandex/cloud/compute/v1"
	"github.com/yandex-cloud/go-genproto/yandex/cloud/iam/v1"
	ydbv1 "github.com/yandex-cloud/go-genproto/yandex/cloud/ydb/v1"
	ycsdk "github.com/yandex-cloud/go-sdk"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"
	"ya-ansible-inventory/workspaces"
)

var (
	instancePerPage int64 = 256
	args            argsT
	errNat          = errors.New("Nat IP not found")
	errEnv          = errors.New("Env variable not set")
	errDBnotFound   = errors.New("Database not found")
	errSetState     = errors.New("Set state. Must set ws Name and ws State")
)

type argsT struct {
	List        bool
	Host        string
	Ssh         bool
	SshUser     string
	SshPort     int
	SshNatGroup string
	DbList      bool
	DbCreate    string
	DbSet       string
}

type sshConf struct {
	User   string
	GW     string
	GWPort int
}

type ansibleGroup struct {
	Hosts    []string               `json:"hosts,omitempty"`
	Vars     map[string]string      `json:"vars,omitempty"`
	HostVars map[string]ansibleVars `json:"hostvars,omitempty"`
}

type ansibleInventory map[string]ansibleGroup
type ansibleVars map[string]string

func main() {
	flag.BoolVar(&args.List, "list", false, "List inventory")
	flag.StringVar(&args.Host, "host", "", "Get Host vars")
	flag.BoolVar(&args.Ssh, "ssh", false, "Get ssh.conf")
	flag.BoolVar(&args.DbList, "db-list", false, "DB List workspaces")
	flag.StringVar(&args.DbCreate, "db-create", "", "DB Create WorkSpace")
	flag.StringVar(&args.DbSet, "db-set", "", "DB Set State")
	flag.StringVar(&args.SshUser, "ssh-user", "cloud-user", "Set user for ssh.conf")
	flag.StringVar(&args.SshNatGroup, "ssh-nat-group", "nat", "Set nat group for ssh.conf")
	flag.IntVar(&args.SshPort, "ssh-port", 22, "Set GW port for ssh.conf")
	flag.Parse()

	if args.List {
		ai, err := newAnsibleInventory()
		if err != nil {
			log.Fatal(err)
		}
		ai.print()
	} else if args.DbList {
		dbList()
	} else if len(args.DbCreate) > 0 {
		dbCreate(args.DbCreate)
	} else if len(args.DbSet) > 0 {
		dbSetState(args.DbSet)
	} else if args.Ssh {
		getSshConf()
	} else if len(args.Host) > 0 {
		ansibleHost(args.Host)
	} else {
		flag.PrintDefaults()
		os.Exit(1)
	}
}

func prepareDB() workspaces.YDBConn {
	db, err := getDB()
	if err != nil {
		log.Fatal(err)
	}
	iam, err := getIAM()
	if err != nil {
		log.Fatal(err)
	}
	return workspaces.YDBConn{
		DatabaseName: db.Name,
		TableName:    "main",
		IAMtoken:     iam,
		Endpoint:     db.Endpoint,
		CTX:          context.Background(),
	}
}

func dbList() {
	ws := prepareDB()
	defer ws.Close()
	r, _ := ws.Select(nil)
	prepareBytes, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(string(prepareBytes))
}

func dbCreate(j string) {
	inRow := workspaces.WsRow{}
	err := json.Unmarshal([]byte(j), &inRow)
	if err != nil {
		log.Fatal(err)
	}
	inRow.State = "creating"
	inRow.CreateDate = time.Now()
	inRow.UpdateDate = time.Now()
	ws := prepareDB()
	defer ws.Close()
	err = ws.CreateTable()
	if err != nil {
		log.Fatal(err)
	}
	err = ws.Insert(inRow)
	if err != nil {
		log.Fatal(err)
	}
}
func dbSetState(j string) {
	inRow := workspaces.WsRow{}
	err := json.Unmarshal([]byte(j), &inRow)
	if err != nil {
		log.Fatal(err)
	}
	if len(inRow.Name) < 1 || len(inRow.State) < 1 {
		log.Fatal(errSetState)
	}
	ws := prepareDB()
	defer ws.Close()
	err = ws.Set(map[string]interface{}{
		"state": inRow.State,
	},
		map[string]interface{}{
			"name": inRow.Name,
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}

func getSshConf() {
	ai, err := newAnsibleInventory()
	if err != nil {
		log.Fatal(err)
	}
	natList, ok := ai[args.SshNatGroup]
	if !ok {
		log.Fatal(errNat)
	}
	if len(natList.Hosts) < 1 {
		log.Fatal(errNat)
	}
	natHost := natList.Hosts[0]
	natHostIp, ok := ai["_meta"].HostVars[natHost]["public_address"]
	if !ok {
		log.Fatal(errNat)
	}
	conf := sshConf{
		User:   args.SshUser,
		GW:     natHostIp,
		GWPort: args.SshPort,
	}
	t, err := template.New("todos").Parse("Host *\n  User={{ .User }}\n  ProxyCommand=ssh -o StrictHostKeyChecking=no -q -W %h:%p {{ .User }}@{{ .GW }} -p{{ .GWPort }}\n")
	if err != nil {
		log.Fatal(err)
	}
	err = t.Execute(os.Stdout, conf)
	if err != nil {
		log.Fatal(err)
	}
}

func (ai *ansibleInventory) print() {
	prepareBytes, err := json.MarshalIndent(ai, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(string(prepareBytes))
}

func checkEnvs(envLabels []string) (map[string]string, error) {

	envs := map[string]string{}
	for _, e := range envLabels {
		envValue := os.Getenv(e)
		if len(envValue) < 1 {
			return nil, errEnv
		}
		envs[e] = envValue
	}
	return envs, nil
}

func newAnsibleInventory() (ansibleInventory, error) {
	envLabels := []string{"YC_TOKEN", "FOLDER_ID", "WORKSPACE"}
	envs, err := checkEnvs(envLabels)
	if err != nil {
		log.Fatal("You must set this ENVs: ", strings.Join(envLabels, ", "))
	}
	ctx := context.Background()

	sdk, err := ycsdk.Build(ctx, ycsdk.Config{
		Credentials: ycsdk.OAuthToken(envs["YC_TOKEN"]),
	})
	if err != nil {
		return nil, err
	}
	instances, err := getInstances(ctx, sdk, envs["FOLDER_ID"])
	if err != nil {
		return nil, err
	}
	ansibleInventory := ansibleInventory{}
	ansibleMeta := map[string]ansibleVars{}
	for _, i := range instances {
		if wsVal, ok := i.Labels["workspace"]; ok {
			if wsVal != envs["WORKSPACE"] {
				continue
			}
		} else {
			continue
		}

		groupAllItem := ansibleInventory["all"]
		groupAllItem.Hosts = append(groupAllItem.Hosts, i.Name)
		sort.Strings(groupAllItem.Hosts)
		ansibleInventory["all"] = groupAllItem
		ansibleMeta[i.Name] = ansibleVars{"ansible_host": i.GetNetworkInterfaces()[0].GetPrimaryV4Address().Address}
		if len(i.GetNetworkInterfaces()[0].GetPrimaryV4Address().GetOneToOneNat().GetAddress()) > 0 {
			ansibleMeta[i.Name]["public_address"] = i.GetNetworkInterfaces()[0].GetPrimaryV4Address().GetOneToOneNat().GetAddress()
		}

		group, ok := i.Labels["group"]
		if !ok {
			continue
		}
		groupItem := ansibleInventory[group]
		groupItem.Hosts = append(groupItem.Hosts, i.Name)
		sort.Strings(groupItem.Hosts)
		groupItem.Vars = mergeKeys(groupItem.Vars, renameLabels("tf_", i.Labels))
		ansibleInventory[group] = groupItem
	}

	for k, v := range ansibleInventory {
		if k == "all" || k == "_meta" {
			continue
		}
		for i, h := range v.Hosts {
			ansibleMeta[h][fmt.Sprintf("tf_group_%s_id", k)] = strconv.Itoa(i)
		}
	}

	metaGroup := ansibleGroup{}
	metaGroup.HostVars = ansibleMeta
	ansibleInventory["_meta"] = metaGroup
	return ansibleInventory, nil
}

func ansibleHost(h string) {
	// Print default answer with meta
	dump := `{"_meta": {"hostvars": {} } }`
	var obj map[string]interface{}
	json.Unmarshal([]byte(dump), &obj)
	prepareBytes, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(string(prepareBytes))
}

func renameLabels(prefix string, in map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range in {
		out[prefix+k] = v
	}
	return out
}

// Given two maps, recursively merge right into left, NEVER replacing any key that already exists in left
func mergeKeys(left, right map[string]string) map[string]string {
	if left == nil {
		left = map[string]string{}
	}
	if right == nil {
		return left
	}
	for key, rightVal := range right {
		if _, present := left[key]; !present {
			left[key] = rightVal
		}
	}
	return left
}

func getInstances(ctx context.Context, sdk *ycsdk.SDK, folderID string) ([]*compute.Instance, error) {
	var instances []*compute.Instance
	req := &compute.ListInstancesRequest{
		FolderId: folderID,
		PageSize: instancePerPage,
	}
	res, err := sdk.Compute().Instance().List(ctx, req)
	if err != nil {
		return nil, err
	}
	instances = res.Instances
	for len(res.NextPageToken) > 0 {
		req := &compute.ListInstancesRequest{
			FolderId:  folderID,
			PageSize:  instancePerPage,
			PageToken: res.NextPageToken,
		}
		res, err = sdk.Compute().Instance().List(ctx, req)
		if err != nil {
			return nil, err
		}
		instances = append(instances, res.Instances...)
	}
	return instances, nil
}

func getIAM() (string, error) {
	envLabels := []string{"YC_TOKEN"}
	envs, err := checkEnvs(envLabels)
	if err != nil {
		log.Fatal("You must set this ENVs: ", strings.Join(envLabels, ", "))
	}
	ctx := context.Background()

	sdk, err := ycsdk.Build(ctx, ycsdk.Config{
		Credentials: ycsdk.OAuthToken(envs["YC_TOKEN"]),
	})
	if err != nil {
		return "", err
	}
	responce, err := sdk.IAM().IamToken().Create(ctx, &iam.CreateIamTokenRequest{Identity: &iam.CreateIamTokenRequest_YandexPassportOauthToken{YandexPassportOauthToken: envs["YC_TOKEN"]}})
	return responce.IamToken, err
}

func getDB() (*ydbv1.Database, error) {
	envLabels := []string{"YC_TOKEN", "FOLDER_ID", "YC_DB"}
	envs, err := checkEnvs(envLabels)
	if err != nil {
		log.Fatal("You must set this ENVs: ", strings.Join(envLabels, ", "))
	}
	ctx := context.Background()

	sdk, err := ycsdk.Build(ctx, ycsdk.Config{
		Credentials: ycsdk.OAuthToken(envs["YC_TOKEN"]),
	})
	if err != nil {
		return nil, err
	}
	ydbs, err := getYDBs(ctx, sdk, envs["FOLDER_ID"])
	if err != nil {
		return nil, err
	}
	for _, db := range ydbs {
		if db.Name == envs["YC_DB"] {
			return db, nil
		}
	}
	return nil, errDBnotFound
}

func getYDBs(ctx context.Context, sdk *ycsdk.SDK, folderID string) ([]*ydbv1.Database, error) {
	var ydbs []*ydbv1.Database
	req := &ydbv1.ListDatabasesRequest{
		FolderId: folderID,
		PageSize: instancePerPage,
	}
	res, err := sdk.YDB().Database().List(ctx, req)
	if err != nil {
		return nil, err
	}
	ydbs = res.Databases
	for len(res.NextPageToken) > 0 {
		req := &ydbv1.ListDatabasesRequest{
			FolderId:  folderID,
			PageSize:  instancePerPage,
			PageToken: res.NextPageToken,
		}
		res, err := sdk.YDB().Database().List(ctx, req)
		if err != nil {
			return nil, err
		}
		ydbs = append(ydbs, res.Databases...)
	}
	return ydbs, nil
}
