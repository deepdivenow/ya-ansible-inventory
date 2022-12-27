package dynamoDB

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	awsSess "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"strings"
	"time"
	"ya-ansible-inventory/cloud"
	"ya-ansible-inventory/common"
)

var (
	errDBnotFound = errors.New("Database not found")
	errSetState   = errors.New("Set state. Must set ws Name and ws State")
)

func MakeCloudDBAWS(cl cloud.Cloud) (*dynamoDB, error) {
	envLabels := []string{"AWS_TABLE"}
	envs, err := common.CheckEnvs(envLabels)
	if err != nil {
		return nil, fmt.Errorf("You must set this ENVs: %s", strings.Join(envLabels, ", "))
	}
	sess, err := awsSess.NewSessionWithOptions(awsSess.Options{
		SharedConfigState: awsSess.SharedConfigEnable,
	})
	if err != nil {
		return nil, err
	}
	// Create DynamoDB client
	client := dynamodb.New(sess)
	conn := &DDBConn{
		TableName: envs["AWS_TABLE"],
		CTX:       context.Background(),
		client:    client,
	}
	return &dynamoDB{api: client, conn: conn}, nil
}

type dynamoDB struct {
	api  *dynamodb.DynamoDB
	conn *DDBConn
}

func (d dynamoDB) List() error {
	r, err := d.conn.Select(nil)
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

func (d dynamoDB) Create(s string) error {
	inRow := WsRow{}
	err := json.Unmarshal([]byte(s), &inRow)
	if err != nil {
		return err
	}
	inRow.State = "creating"
	inRow.CreateDate = time.Now()
	inRow.UpdateDate = time.Now()
	err = d.conn.CreateTable()
	if err != nil {
		return err
	}
	return d.conn.Insert(inRow)
}

func (d dynamoDB) SetState(s string) error {
	inRow := WsRow{}
	err := json.Unmarshal([]byte(s), &inRow)
	if err != nil {
		return err
	}
	if len(inRow.Name) < 1 || len(inRow.State) < 1 {
		return errSetState
	}
	return d.conn.Set(inRow.Name, inRow.State)
}

func (d dynamoDB) Close() {
	//if d.conn != nil {
	//	d.conn.Close()
	//	d.conn = nil
	//}
}

type WsRow struct {
	Name       string    `json:"name"`
	NetId      uint32    `json:"net_id"`
	CreateDate time.Time `json:"create_date"`
	UpdateDate time.Time `json:"update_date"`
	State      string    `json:"state"`
	HaMode     bool      `json:"ha_mode"`
}
