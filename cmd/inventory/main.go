package main

import (
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
	cl "ya-ansible-inventory/cloud"
	"ya-ansible-inventory/cloudDB"
	ch "ya-ansible-inventory/cloudHelper"
	"ya-ansible-inventory/common"
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
	Vars     map[string]interface{} `json:"vars,omitempty"`
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
	envLabels := []string{"CLOUD_TYPE"}
	envs, err := common.CheckEnvs(envLabels)
	if err != nil {
		log.Fatal("You must set this ENVs: %s", strings.Join(envLabels, ", "))
	}
	if args.List {
		ai, err := newAnsibleInventory()
		if err != nil {
			log.Fatal(err)
		}
		ai.print()
	} else if args.DbList || len(args.DbCreate) > 0 || len(args.DbSet) > 0 {
		//dbList()
		cloud, err := ch.MakeCloud(envs["CLOUD_TYPE"])
		if err != nil {
			log.Fatal(err)
		}
		db, err := cloudDB.MakeCloudDB(cloud, envs["CLOUD_TYPE"])
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()
		if args.DbList {
			err = db.List()
			if err != nil {
				log.Fatal(err)
			}
		} else if len(args.DbCreate) > 0 {
			err = db.Create(args.DbCreate)
			if err != nil {
				log.Fatal(err)
			}
		} else if len(args.DbSet) > 0 {
			err = db.SetState(args.DbSet)
			if err != nil {
				log.Fatal(err)
			}
		}
	} else if args.Ssh {
		err = getSshConf()
		if err != nil {
			log.Fatal(err)
		}
	} else if len(args.Host) > 0 {
		err = ansibleHost(args.Host)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		flag.PrintDefaults()
		os.Exit(1)
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
	//TODO Check ENV vars on begining
	envLabels := []string{"WORKSPACE", "CLOUD_TYPE"}
	envs, err := common.CheckEnvs(envLabels)
	if err != nil {
		return nil, fmt.Errorf("You must set this ENVs: %s", strings.Join(envLabels, ", "))
	}

	cloud, err := ch.MakeCloud(envs["CLOUD_TYPE"])
	if err != nil {
		return nil, err
	}
	wsFilter := &cl.LabelFilter{
		LabelEqual: map[string]string{"workspace": envs["WORKSPACE"]},
	}
	instances, err := cloud.GetInstances(wsFilter)
	if err != nil {
		return nil, err
	}
	ansibleInventory := ansibleInventory{}
	ansibleMeta := map[string]ansibleVars{}
	for _, i := range instances {
		iLabels := i.GetLabels()
		groupAllItem := ansibleInventory["all"]
		iName := i.GetName()
		groupAllItem.Hosts = append(groupAllItem.Hosts, iName)
		sort.Strings(groupAllItem.Hosts)
		ansibleInventory["all"] = groupAllItem
		iIfases := i.GetInterfaces()
		if len(iIfases.Private) > 0 {
			ansibleMeta[iName] = ansibleVars{"ansible_host": iIfases.Private[0]}
		}
		if len(iIfases.Public) > 0 && len(iIfases.Public[0]) > 0 {
			ansibleMeta[iName]["public_address"] = iIfases.Public[0]
		}

		group, ok := iLabels["group"]
		if !ok {
			continue
		}
		groupItem := ansibleInventory[group]
		groupItem.Hosts = append(groupItem.Hosts, iName)
		sort.Strings(groupItem.Hosts)
		groupItem.Vars = common.MergeKeys(groupItem.Vars, common.RenameKeys("tf_", iLabels))
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
	// Add subnet vars to nat group
	if natGroup, ok := ansibleInventory["nat"]; ok {
		subnets, err := cloud.GetSubnets(wsFilter)
		if err != nil {
			return ansibleInventory, nil
		}
		var cidrBlocks []string
		for _, s := range subnets {
			cidrBlocks = append(cidrBlocks, s.GetCidrs()...)
		}
		natGroup.Vars["tf_subnets"] = cidrBlocks
	}
	return ansibleInventory, nil
}

func ansibleHost(h string) error {
	// Print default answer with meta
	dump := `{"_meta": {"hostvars": {} } }`
	var obj map[string]interface{}
	json.Unmarshal([]byte(dump), &obj)
	prepareBytes, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return err
	}
	fmt.Print(string(prepareBytes))
	return nil
}

func getSshConf() error {
	ai, err := newAnsibleInventory()
	if err != nil {
		return err
	}
	natList, ok := ai[args.SshNatGroup]
	if !ok {
		return errNat
	}
	if len(natList.Hosts) < 1 {
		return errNat
	}
	natHost := natList.Hosts[0]
	natHostIp, ok := ai["_meta"].HostVars[natHost]["public_address"]
	if !ok {
		return errNat
	}
	conf := sshConf{
		User:   args.SshUser,
		GW:     natHostIp,
		GWPort: args.SshPort,
	}
	t, err := template.New("todos").Parse("Host *\n  User={{ .User }}\n  ProxyCommand=ssh -o StrictHostKeyChecking=no -q -W %h:%p {{ .User }}@{{ .GW }} -p{{ .GWPort }}\n")
	if err != nil {
		return err

	}
	err = t.Execute(os.Stdout, conf)
	if err != nil {
		return err
	}
	return nil
}
