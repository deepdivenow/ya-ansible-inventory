package cloudDB

import (
	"fmt"
	"ya-ansible-inventory/cloud"
	"ya-ansible-inventory/cloudDB/dynamoDB"
	"ya-ansible-inventory/cloudDB/ydb"
)

type CloudDB interface {
	List() error
	Create(string) error
	SetState(string) error
	Close()
}

func MakeCloudDB(cl cloud.Cloud, t string) (CloudDB, error) {
	switch t {
	case "yandex", "yacloud", "ydb":
		return ydb.MakeCloudDBYandex(cl)
	case "aws":
		return dynamoDB.MakeCloudDBAWS(cl)
	default:
		return nil, fmt.Errorf("Not have implemented yet")
	}
}
