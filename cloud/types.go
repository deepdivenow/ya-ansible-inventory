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

type Cloud interface {
	GetInstances(filter Filter) ([]Host, error)
	GetVpcs(filter Filter) ([]VPC, error)
	GetSubnets(filter Filter) ([]Subnet, error)
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

type EnvState interface {
}
