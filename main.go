package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/yandex-cloud/go-genproto/yandex/cloud/iam/v1"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/yandex-cloud/go-genproto/yandex/cloud/compute/v1"
	ycsdk "github.com/yandex-cloud/go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/table"
)

var (
	instancePerPage int64 = 256
	args            argsT
	errNat          = errors.New("Nat IP not found")
	errEnv = errors.New("Env variable not set")
)

type argsT struct {
	List        bool
	Host        string
	Ssh         bool
	SshUser     string
	SshPort     int
	SshNatGroup string
	Db bool
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
	flag.BoolVar(&args.Db, "db", false, "DB control")
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
	} else if args.Db {
		newDBs()
	} else if args.Ssh {
		getSshConf()
	} else if len(args.Host) > 0 {
		ansibleHost(args.Host)
	} else {
		flag.PrintDefaults()
		os.Exit(1)
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
	natHostIp,ok := ai["_meta"].HostVars[natHost]["public_address"]
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

func checkEnvs(envLabels []string) (map[string]string,error){

	envs := map[string]string{}
	for _, e := range envLabels {
		envValue := os.Getenv(e)
		if len(envValue) < 1 {
			return nil, errEnv
		}
		envs[e] = envValue
	}
	return envs,nil
}

func newAnsibleInventory() (ansibleInventory, error) {
	envLabels := []string{"YC_TOKEN", "FOLDER_ID", "WORKSPACE"}
	envs,err:=checkEnvs(envLabels)
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

func getIAM() (string,error) {
	envLabels := []string{"YC_TOKEN"}
	envs,err:=checkEnvs(envLabels)
	if err != nil {
		log.Fatal("You must set this ENVs: ", strings.Join(envLabels, ", "))
	}
	ctx := context.Background()

	sdk, err := ycsdk.Build(ctx, ycsdk.Config{
		Credentials: ycsdk.OAuthToken(envs["YC_TOKEN"]),
	})
	if err != nil {
		return "",err
	}
	responce,err:=sdk.IAM().IamToken().Create(ctx,&iam.CreateIamTokenRequest{Identity: &iam.CreateIamTokenRequest_YandexPassportOauthToken{YandexPassportOauthToken: envs["YC_TOKEN"]}})
	return responce.IamToken,err
}

func newDBs() error {
	ctx:=context.Background()
	iamToken,err := getIAM()
	if err != nil {
		log.Fatal(err)
	}
	dbPath := "/ru-central1/b1gkhnudnlgr0ek88c5t/etn012c5v78f3ratmu5n"
	dialer := &ydb.Dialer{
		DriverConfig: &ydb.DriverConfig{
			Database: dbPath,
			Credentials: ydb.AuthTokenCredentials{
				AuthToken: iamToken,
			},
		},
		TLSConfig:    &tls.Config{/*...*/},
		Timeout:      time.Second,
	}
	driver, err := dialer.Dial(ctx, "ydb.serverless.yandexcloud.net:2135")
	if err != nil {
		log.Fatal(err)
	}
	tc := table.Client{
		Driver: driver,
	}
	s, err := tc.CreateSession(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close(ctx)
	err = s.CreateTable(ctx,path.Join(dbPath,"main"),
		table.WithColumn("id",ydb.Optional(ydb.TypeUint32)),
		table.WithColumn("name",ydb.Optional(ydb.TypeString)),
		table.WithColumn("net_id",ydb.Optional(ydb.TypeUint32)),
		table.WithColumn("create_date",ydb.Optional(ydb.TypeTimestamp)),
		table.WithColumn("update_date",ydb.Optional(ydb.TypeTimestamp)),
		table.WithColumn("state",ydb.Optional(ydb.TypeString)),
		table.WithPrimaryKeyColumn("id"),
	)
	if err != nil {
		log.Fatal(err)
	}

	_,res,err:=s.Execute(ctx,nil,"select 1")
	println(res)


//	INSERT INTO episodes
//	(
//		series_id,
//		season_id,
//		episode_id,
//		title,
//		air_date
//	)
//	VALUES
//	(
//		2,
//		5,
//		21,
//		"Test 21",
//		CAST(Date("2018-08-27") AS Uint64)
//)
//	;
	return nil
}
//
//func render(t *template.Template, data interface{}) string {
//	var buf bytes.Buffer
//	err := t.Execute(&buf, data)
//	if err != nil {
//		panic(err)
//	}
//	return buf.String()
//}
//
//func selectSimple(ctx context.Context, sp *table.SessionPool, prefix string) (err error) {
//	query := render(
//		template.Must(template.New("").Parse(`
//			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
//			DECLARE $seriesID AS Uint64;
//			$format = DateTime::Format("%Y-%m-%d");
//			SELECT
//				series_id,
//				title,
//				$format(DateTime::FromSeconds(CAST(DateTime::ToSeconds(DateTime::IntervalFromDays(CAST(release_date AS Int16))) AS Uint32))) AS release_date
//			FROM
//				series
//			WHERE
//				series_id = $seriesID;
//		`)),
//		templateConfig{
//			TablePathPrefix: prefix,
//		},
//	)
//	readTx := table.TxControl(
//		table.BeginTx(
//			table.WithOnlineReadOnly(),
//		),
//		table.CommitTx(),
//	)
//	var res *table.Result
//	err = table.Retry(ctx, sp,
//		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
//			_, res, err = s.Execute(ctx, readTx, query,
//				table.NewQueryParameters(
//					table.ValueParam("$seriesID", ydb.Uint64Value(1)),
//				),
//				table.WithQueryCachePolicy(
//					table.WithQueryCachePolicyKeepInCache(),
//				),
//				table.WithCollectStatsModeBasic(),
//			)
//			return
//		}),
//	)
//	if err != nil {
//		return err
//	}
//	for res.NextSet() {
//		for res.NextRow() {
//			res.SeekItem("series_id")
//			id := res.OUint64()
//
//			res.NextItem()
//			title := res.OUTF8()
//
//			res.NextItem()
//			date := res.OString()
//
//			log.Printf(
//				"\n> select_simple_transaction: %d %s %s",
//				id, title, date,
//			)
//		}
//	}
//	if err := res.Err(); err != nil {
//		return err
//	}
//	return nil
//}