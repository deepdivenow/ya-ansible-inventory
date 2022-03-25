package yandex

import (
	"context"
	"fmt"
	"github.com/yandex-cloud/go-genproto/yandex/cloud/compute/v1"
	"github.com/yandex-cloud/go-genproto/yandex/cloud/vpc/v1"
	ycsdk "github.com/yandex-cloud/go-sdk"
	"strings"
	"ya-ansible-inventory/cloud"
	"ya-ansible-inventory/common"
)

const (
	instancePerPage = 100
)

func MakeCloudYandex() (*CloudYandex, error) {
	ctx := context.TODO()
	envLabels := []string{"YC_TOKEN", "FOLDER_ID"}
	envs, err := common.CheckEnvs(envLabels)
	if err != nil {
		return nil, fmt.Errorf("You must set this ENVs: %s", strings.Join(envLabels, ", "))
	}
	client, err := ycsdk.Build(ctx, ycsdk.Config{
		Credentials: ycsdk.OAuthToken(envs["YC_TOKEN"]),
	})
	if err != nil {
		return nil, err
	}
	return &CloudYandex{api: client, folderId: envs["FOLDER_ID"]}, nil
}

type CloudYandex struct {
	api      *ycsdk.SDK
	folderId string
}

type HostYandex compute.Instance
type VpcYandex vpc.Network
type SubnetYandex vpc.Subnet

func (h *HostYandex) GetName() string {
	return h.Name
}

func (h *HostYandex) GetId() string {
	return h.Id
}

func (h *HostYandex) GetLabels() map[string]string {
	return h.Labels
}

func (h *HostYandex) GetInterfaces() cloud.Iface {
	var pubIfs, priIfs []string
	ifaces := h.NetworkInterfaces
	for _, i := range ifaces {
		priIfs = append(priIfs, i.PrimaryV4Address.Address)
		if len(i.GetPrimaryV4Address().GetOneToOneNat().GetAddress()) > 0 {
			pubIfs = append(pubIfs, i.GetPrimaryV4Address().GetOneToOneNat().GetAddress())
		}
	}
	return cloud.Iface{
		Public:  pubIfs,
		Private: priIfs,
	}
}

func (v *VpcYandex) GetName() string {
	return v.Name
}

func (v *VpcYandex) GetId() string {
	return v.Id
}

func (v *VpcYandex) GetLabels() map[string]string {
	return v.Labels
}

func (s *SubnetYandex) GetName() string {
	return s.Name
}

func (s *SubnetYandex) GetId() string {
	return s.Id
}

func (s *SubnetYandex) GetLabels() map[string]string {
	return s.Labels
}

func (s *SubnetYandex) GetVPCId() string {
	return s.NetworkId
}

func (s *SubnetYandex) GetCidrs() []string {
	return s.V4CidrBlocks
}

func (y *CloudYandex) GetInstances(filter cloud.Filter) ([]cloud.Host, error) {
	instances, err := y.getInstances()
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

func (y *CloudYandex) getInstances() ([]*HostYandex, error) {
	var result []*HostYandex
	ctx := context.TODO()
	pageToken := ""
	for {
		resp, err := y.api.Compute().Instance().List(ctx, &compute.ListInstancesRequest{
			FolderId:  y.folderId,
			PageSize:  instancePerPage,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, err
		}
		for _, i := range resp.Instances {
			h := HostYandex(*i)
			result = append(result, &h)
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return result, nil
}

func (y *CloudYandex) GetVpcs(filter cloud.Filter) ([]cloud.VPC, error) {
	vpcs, err := y.getVpcs()
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

func (y *CloudYandex) getVpcs() ([]*VpcYandex, error) {
	var result []*VpcYandex
	pageToken := ""
	ctx := context.TODO()
	for {
		resp, err := y.api.VPC().Network().List(ctx, &vpc.ListNetworksRequest{
			FolderId:  y.folderId,
			PageSize:  instancePerPage,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, err
		}
		for _, net := range resp.GetNetworks() {
			n := VpcYandex(*net)
			result = append(result, &n)
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return result, nil
}

func (y *CloudYandex) GetSubnets(filter cloud.Filter) ([]cloud.Subnet, error) {
	subs, err := y.getSubNets()
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

func (y *CloudYandex) getSubNets() ([]*SubnetYandex, error) {
	var result []*SubnetYandex
	ctx := context.TODO()
	pageToken := ""
	for {
		resp, err := y.api.VPC().Subnet().List(ctx, &vpc.ListSubnetsRequest{
			FolderId:  y.folderId,
			PageSize:  instancePerPage,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, err
		}
		for _, sub := range resp.GetSubnets() {
			s := SubnetYandex(*sub)
			result = append(result, &s)
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return result, nil
}

//
//func getIAM() (string, error) {
//	envLabels := []string{"YC_TOKEN"}
//	envs, err := checkEnvs(envLabels)
//	if err != nil {
//		log.Fatal("You must set this ENVs: ", strings.Join(envLabels, ", "))
//	}
//	ctx := context.Background()
//
//	sdk, err := ycsdk.Build(ctx, ycsdk.Config{
//		Credentials: ycsdk.OAuthToken(envs["YC_TOKEN"]),
//	})
//	if err != nil {
//		return "", err
//	}
//	responce, err := sdk.IAM().IamToken().Create(ctx, &iam.CreateIamTokenRequest{Identity: &iam.CreateIamTokenRequest_YandexPassportOauthToken{YandexPassportOauthToken: envs["YC_TOKEN"]}})
//	return responce.IamToken, err
//}
//
//func getDB() (*ydbv1.Database, error) {
//	envLabels := []string{"YC_TOKEN", "FOLDER_ID", "YC_DB"}
//	envs, err := checkEnvs(envLabels)
//	if err != nil {
//		log.Fatal("You must set this ENVs: ", strings.Join(envLabels, ", "))
//	}
//	ctx := context.Background()
//
//	sdk, err := ycsdk.Build(ctx, ycsdk.Config{
//		Credentials: ycsdk.OAuthToken(envs["YC_TOKEN"]),
//	})
//	if err != nil {
//		return nil, err
//	}
//	ydbs, err := getYDBs(ctx, sdk, envs["FOLDER_ID"])
//	if err != nil {
//		return nil, err
//	}
//	for _, db := range ydbs {
//		if db.Name == envs["YC_DB"] {
//			return db, nil
//		}
//	}
//	return nil, errDBnotFound
//}
//
//func getYDBs(ctx context.Context, sdk *ycsdk.SDK, folderID string) ([]*ydbv1.Database, error) {
//	var ydbs []*ydbv1.Database
//	req := &ydbv1.ListDatabasesRequest{
//		FolderId: folderID,
//		PageSize: instancePerPage,
//	}
//	res, err := sdk.YDB().Database().List(ctx, req)
//	if err != nil {
//		return nil, err
//	}
//	ydbs = res.Databases
//	for len(res.NextPageToken) > 0 {
//		req := &ydbv1.ListDatabasesRequest{
//			FolderId:  folderID,
//			PageSize:  instancePerPage,
//			PageToken: res.NextPageToken,
//		}
//		res, err := sdk.YDB().Database().List(ctx, req)
//		if err != nil {
//			return nil, err
//		}
//		ydbs = append(ydbs, res.Databases...)
//	}
//	return ydbs, nil
//}
