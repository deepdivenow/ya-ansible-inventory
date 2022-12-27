package ydb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	iamv1 "github.com/yandex-cloud/go-genproto/yandex/cloud/iam/v1"
	ycsdk "github.com/yandex-cloud/go-sdk"
	"strings"
	"time"
	"ya-ansible-inventory/cloud"
	"ya-ansible-inventory/common"
)

var (
	errDBnotFound = errors.New("Database not found")
	errSetState   = errors.New("Set state. Must set ws Name and ws State")
)

func MakeCloudDBYandex(cl cloud.Cloud) (*CdbYandex, error) {
	ctx := context.TODO()
	envLabels := []string{"YC_TOKEN", "FOLDER_ID", "YC_DB"}
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
	dbs, err := cl.GetDBs(&cloud.NameFilter{NameEqual: envs["YC_DB"]})
	if err != nil {
		return nil, err
	}
	if len(dbs) < 1 {
		return nil, fmt.Errorf("DBs not found")
	}
	iam, err := client.IAM().IamToken().Create(ctx, &iamv1.CreateIamTokenRequest{Identity: &iamv1.CreateIamTokenRequest_YandexPassportOauthToken{YandexPassportOauthToken: envs["YC_TOKEN"]}})
	if err != nil {
		return nil, err
	}
	conn := &YDBConn{
		DatabaseName: dbs[0].GetName(),
		TableName:    "main",
		IAMtoken:     iam.IamToken,
		Endpoint:     dbs[0].GetEndpoint(),
		CTX:          context.Background(),
	}
	return &CdbYandex{api: client, conn: conn, folderId: envs["FOLDER_ID"]}, nil
}

type WsRow struct {
	Name       string    `json:"name"`
	NetId      uint32    `json:"net_id"`
	CreateDate time.Time `json:"create_date"`
	UpdateDate time.Time `json:"update_date"`
	State      string    `json:"state"`
	HaMode     bool      `json:"ha_mode"`
}

type CdbYandex struct {
	api      *ycsdk.SDK
	folderId string
	conn     *YDBConn
}

func (y *CdbYandex) Close() {
	if y.conn != nil {
		y.conn.Close()
		y.conn = nil
	}
}

func (y *CdbYandex) List() error {
	r, err := y.conn.Select(nil)
	if err != nil {
		return err
	}
	s, err := common.StringDumpErr(r)
	if err != nil {
		return err
	}
	fmt.Println(s)
	return nil
}

func (y *CdbYandex) Create(j string) error {
	inRow := WsRow{}
	err := json.Unmarshal([]byte(j), &inRow)
	if err != nil {
		return err
	}
	inRow.State = "creating"
	inRow.CreateDate = time.Now()
	inRow.UpdateDate = time.Now()
	err = y.conn.CreateTable()
	if err != nil {
		return err
	}
	err = y.conn.Insert(inRow)
	if err != nil {
		return err
	}
	return nil
}

func (y *CdbYandex) SetState(j string) error {
	inRow := WsRow{}
	err := json.Unmarshal([]byte(j), &inRow)
	if err != nil {
		return err
	}
	if len(inRow.Name) < 1 || len(inRow.State) < 1 {
		return errSetState
	}
	err = y.conn.Set(map[string]interface{}{
		"state": inRow.State,
	},
		map[string]interface{}{
			"name": inRow.Name,
		},
	)
	if err != nil {
		return err
	}
	return nil
}
