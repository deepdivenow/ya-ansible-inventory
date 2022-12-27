package aws

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2Type "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"ya-ansible-inventory/cloud"
	"ya-ansible-inventory/common"
)

func MakeCloudAWS() (*CloudAWS, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}
	client := ec2.NewFromConfig(cfg)
	return &CloudAWS{api: client}, nil
}

type CloudAWS struct {
	api *ec2.Client
}

type HostAWS ec2Type.Instance
type VpcAWS ec2Type.Vpc
type SubnetAWS ec2Type.Subnet

func (h *HostAWS) GetName() string {
	l := h.GetLabels()
	return l["Name"]
}

func (h *HostAWS) GetId() string {
	return common.StringClone(*h.InstanceId)
}

func (h *HostAWS) GetLabels() map[string]string {
	return tagsToMap(h.Tags)
}

func (h *HostAWS) GetInterfaces() cloud.Iface {
	var pubIfs, priIfs []string
	pubIfs = append(pubIfs, common.StringClone(*h.PublicIpAddress))
	priIfs = append(priIfs, common.StringClone(*h.PrivateIpAddress))
	return cloud.Iface{
		Public:  pubIfs,
		Private: priIfs,
	}
}

func (v *VpcAWS) GetName() string {
	l := v.GetLabels()
	return l["Name"]
}

func (v *VpcAWS) GetId() string {
	return common.StringClone(*v.VpcId)
}

func (v *VpcAWS) GetLabels() map[string]string {
	return tagsToMap(v.Tags)
}

func (s *SubnetAWS) GetName() string {
	l := s.GetLabels()
	return l["Name"]
}

func (s *SubnetAWS) GetId() string {
	return common.StringClone(*s.SubnetId)
}

func (s *SubnetAWS) GetLabels() map[string]string {
	return tagsToMap(s.Tags)
}

func (s *SubnetAWS) GetVPCId() string {
	return common.StringClone(*s.VpcId)
}

func (s *SubnetAWS) GetCidrs() []string {
	return []string{common.StringClone(*s.CidrBlock)}
}

func tagsToMap(tags []ec2Type.Tag) map[string]string {
	res := map[string]string{}
	for _, tag := range tags {
		k := common.StringClone(*tag.Key)
		v := common.StringClone(*tag.Value)
		res[k] = v
	}
	return res
}

func (ca *CloudAWS) GetInstances(filter cloud.Filter) ([]cloud.Host, error) {
	instances, err := ca.getInstances()
	if err != nil {
		return nil, err
	}
	if filter == nil {
		filter = &cloud.DefaultFilter{}
	}
	var res []cloud.Host
	for _, i := range instances {
		if ok := filter.Check(i); ok {
			res = append(res, i)
		}
	}
	return res, nil
}

func (ca *CloudAWS) getInstances() ([]*HostAWS, error) {
	var result []*HostAWS
	out, err := ca.api.DescribeInstances(context.TODO(), nil)
	if err != nil {
		return result, err
	}
	for _, r := range out.Reservations {
		for _, i := range r.Instances {
			h := HostAWS(i)
			result = append(result, &h)
		}
	}
	return result, nil
}

func (ca *CloudAWS) GetVpcs(filter cloud.Filter) ([]cloud.VPC, error) {
	vpcs, err := ca.getVpcs()
	if err != nil {
		return nil, err
	}
	if filter == nil {
		filter = &cloud.DefaultFilter{}
	}
	var res []cloud.VPC
	for _, v := range vpcs {
		if ok := filter.Check(v); ok {
			res = append(res, v)
		}
	}
	return res, nil
}

func (ca *CloudAWS) getVpcs() ([]*VpcAWS, error) {
	var result []*VpcAWS
	out, err := ca.api.DescribeVpcs(context.TODO(), nil)
	if err != nil {
		return result, err
	}
	for _, vpc := range out.Vpcs {
		v := VpcAWS(vpc)
		result = append(result, &v)
	}
	return result, nil
}

func (ca *CloudAWS) GetSubnets(filter cloud.Filter) ([]cloud.Subnet, error) {
	subs, err := ca.getSubNets()
	if err != nil {
		return nil, err
	}
	if filter == nil {
		filter = &cloud.DefaultFilter{}
	}
	var res []cloud.Subnet
	for _, s := range subs {
		if ok := filter.Check(s); ok {
			res = append(res, s)
		}
	}
	return res, nil
}

func (ca *CloudAWS) getSubNets() ([]*SubnetAWS, error) {
	var result []*SubnetAWS
	out, err := ca.api.DescribeSubnets(context.TODO(), nil)
	if err != nil {
		return result, err
	}
	for _, sub := range out.Subnets {
		s := SubnetAWS(sub)
		result = append(result, &s)
	}
	return result, nil
}

func (ca *CloudAWS) GetDBs(filter cloud.Filter) ([]cloud.CloudDB, error) {
	//TODO implement me
	panic("implement me")
}
