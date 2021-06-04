package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/template"

	"github.com/yandex-cloud/go-genproto/yandex/cloud/compute/v1"
	ycsdk "github.com/yandex-cloud/go-sdk"
)

var (
	instancePerPage int64 = 256
	args            argsT
	errNat          = errors.New("Nat IP not found")
)

type argsT struct {
	List        bool
	Host        string
	Ssh         bool
	SshUser     string
	SshPort     int
	SshNatGroup string
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

func newAnsibleInventory() (ansibleInventory, error) {
	envLabels := []string{"YC_TOKEN", "FOLDER_ID", "WORKSPACE"}
	envs := map[string]string{}
	for _, e := range envLabels {
		envValue := os.Getenv(e)
		if len(envValue) < 1 {
			log.Fatal("You must set this ENVs: ", strings.Join(envLabels, ", "))
		}
		envs[e] = envValue
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
