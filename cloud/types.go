package cloud

import "strings"

type Filter interface {
	Check(interface{}) bool
}

type DefaultFilter struct {
}

func (df *DefaultFilter) Check(interface{}) bool {
	return true
}

type LabelChecker interface {
	GetLabels() map[string]string
}

type LabelFilter struct {
	LabelEqual map[string]string
}

func (lf *LabelFilter) Check(i interface{}) bool {
	obj, ok := i.(LabelChecker)
	if !ok {
		return false
	}
	labels := obj.GetLabels()
	for k, v := range lf.LabelEqual {
		if vObj, ok := labels[k]; ok {
			if strings.EqualFold(vObj, v) {
				continue
			}
		}
		return false
	}
	return true
}

type NameChecker interface {
	GetName() string
}

type NameFilter struct {
	NameEqual string
}

func (nf *NameFilter) Check(i interface{}) bool {
	obj, ok := i.(NameChecker)
	if !ok {
		return false
	}
	name := obj.GetName()
	if strings.EqualFold(nf.NameEqual, name) {
		return true
	}
	return false
}

type Cloud interface {
	GetInstances(filter Filter) ([]Host, error)
	GetVpcs(filter Filter) ([]VPC, error)
	GetSubnets(filter Filter) ([]Subnet, error)
	GetDBs(filter Filter) ([]CloudDB, error)
}

type Host interface {
	GetName() string
	GetId() string
	GetLabels() map[string]string
	GetInterfaces() Iface
}

type Iface struct {
	Public  []string
	Private []string
}

type Subnet interface {
	GetName() string
	GetId() string
	GetLabels() map[string]string
	GetVPCId() string
	GetCidrs() []string
}

type VPC interface {
	GetName() string
	GetId() string
	GetLabels() map[string]string
}

type CloudDB interface {
	GetName() string
	GetId() string
	GetLabels() map[string]string
	GetEndpoint() string
}
