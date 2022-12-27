package cloudHelper

import (
	"fmt"
	"ya-ansible-inventory/cloud"
	"ya-ansible-inventory/cloud/aws"
	"ya-ansible-inventory/cloud/yandex"
)

func MakeCloud(t string) (cloud.Cloud, error) {
	switch t {
	case "yandex", "yacloud":
		return yandex.MakeCloudYandex()
	case "aws":
		return aws.MakeCloudAWS()
	default:
		return nil, fmt.Errorf("Not have implemented yet")
	}
}
