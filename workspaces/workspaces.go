package workspaces

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/table"
	"log"
	"net/url"
	"path"
	"time"
)

var (
	errDBBadEndpoint     = errors.New("Database have bad endpoint")
	errDBBadSetOperation = errors.New("Set args error")
)

type YDBConn struct {
	DatabaseName string
	TableName    string
	IAMtoken     string
	Endpoint     string
	CTX          context.Context
	session      *table.Session
}

type WsRow struct {
	Name       string    `json:"name"`
	NetId      uint32    `json:"net_id"`
	CreateDate time.Time `json:"create_date"`
	UpdateDate time.Time `json:"update_date"`
	State      string    `json:"state"`
	HaMode     bool      `json:"ha_mode"`
}

func (y *YDBConn) Close() {
	if y.session != nil {
		y.session.Close(y.CTX)
	}
}

func (y *YDBConn) getDBPath() (string, error) {
	u, err := url.Parse(y.Endpoint)
	if err != nil {
		return "", err
	}
	m, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", err
	}
	dbPath, ok := m["database"]
	if !ok {
		return "", errDBBadEndpoint
	}
	if len(dbPath) < 1 {
		return "", errDBBadEndpoint
	}
	return dbPath[0], nil
}
func (y *YDBConn) getDBAddress() (string, error) {
	u, err := url.Parse(y.Endpoint)
	if err != nil {
		return "", errDBBadEndpoint
	}
	return u.Host, nil
}

func (y *YDBConn) newSession() error {
	dbPath, err := y.getDBPath()
	if err != nil {
		log.Fatal(err)
	}
	dbAddr, err := y.getDBAddress()
	if err != nil {
		log.Fatal(err)
	}
	dialer := &ydb.Dialer{
		DriverConfig: &ydb.DriverConfig{
			Database: dbPath,
			Credentials: ydb.AuthTokenCredentials{
				AuthToken: y.IAMtoken,
			},
		},
		TLSConfig: &tls.Config{ /*...*/ },
		Timeout:   time.Second,
	}
	driver, err := dialer.Dial(y.CTX, dbAddr)
	if err != nil {
		log.Fatal(err)
	}
	tc := table.Client{
		Driver: driver,
	}
	s, err := tc.CreateSession(y.CTX)
	if err != nil {
		log.Fatal(err)
	}
	if y.session != nil {
		y.session.Close(y.CTX)
	}
	y.session = s
	return nil
}

func (y *YDBConn) GetSession() *table.Session {
	if y.session == nil {
		y.newSession()
	}
	return y.session
}

func (y *YDBConn) CreateTable() error {
	dbPath, err := y.getDBPath()
	if err != nil {
		return err
	}
	session := y.GetSession()
	err = session.CreateTable(y.CTX, path.Join(dbPath, y.TableName),
		table.WithColumn("name", ydb.Optional(ydb.TypeString)),
		table.WithColumn("net_id", ydb.Optional(ydb.TypeUint32)),
		table.WithColumn("create_date", ydb.Optional(ydb.TypeTimestamp)),
		table.WithColumn("update_date", ydb.Optional(ydb.TypeTimestamp)),
		table.WithColumn("state", ydb.Optional(ydb.TypeString)),
		table.WithColumn("ha_mode", ydb.Optional(ydb.TypeBool)),
		table.WithPrimaryKeyColumn("name"),
	)
	return err
}

func (y *YDBConn) Insert(r WsRow) error {
	writeTx := table.TxControl(
		table.BeginTx(
			table.WithSerializableReadWrite(),
		),
		table.CommitTx(),
	)
	q := fmt.Sprintf("INSERT INTO %s (name,net_id,state,ha_mode,create_date,update_date) values(%q, %d,%q,%v,DateTime::FromSeconds(%d),DateTime::FromSeconds(%d)) ;",
		y.TableName, r.Name, r.NetId, r.State, r.HaMode, r.CreateDate.Unix(), r.UpdateDate.Unix())
	sess := y.GetSession()
	_, _, err := sess.Execute(y.CTX, writeTx, q, nil)
	return err
}

func mapToQuery(m map[string]interface{}, spliter string) string {
	result := ""
	for k, v := range m {
		if len(result) > 0 {
			result += spliter
		}
		switch v.(type) {
		case string:
			result += fmt.Sprintf("%s = %q", k, v)
		default:
			result += fmt.Sprintf("%s = %v", k, v)
		}
	}
	return result
}

func (y *YDBConn) Set(sf map[string]interface{}, w map[string]interface{}) error {
	writeTx := table.TxControl(
		table.BeginTx(
			table.WithSerializableReadWrite(),
		),
		table.CommitTx(),
	)
	setString := mapToQuery(sf, ", ")
	where := mapToQuery(w, " AND ")
	q := fmt.Sprintf("UPDATE %s SET update_date = DateTime::FromSeconds(%d), %s  WHERE %s ;",
		y.TableName, time.Now().Unix(), setString, where)
	sess := y.GetSession()
	_, _, err := sess.Execute(y.CTX, writeTx, q, nil)

	return err
}

func (y *YDBConn) Select(w map[string]interface{}) (*[]WsRow, error) {
	var result []WsRow
	readTx := table.TxControl(
		table.BeginTx(
			table.WithOnlineReadOnly(),
		),
		table.CommitTx(),
	)
	sess := y.GetSession()
	where := mapToQuery(w, " AND ")
	q := ""
	if len(where) > 0 {
		q = fmt.Sprintf("SELECT name,net_id,state,create_date,update_date FROM %s WHERE %s ;", y.TableName, where)
	} else {
		q = fmt.Sprintf("SELECT name,net_id,state,create_date,update_date FROM %s ;", y.TableName)
	}

	_, res, err := sess.Execute(y.CTX, readTx, q, nil)
	if err != nil {
		log.Fatal(err)
	}
	for res.NextSet() {
		var tmpRow WsRow
		for res.NextRow() {
			res.SeekItem("name")
			tmpRow.Name = string(res.OString())
			res.NextItem()
			tmpRow.NetId = res.OUint32()
			res.NextItem()
			tmpRow.State = string(res.OString())
			res.NextItem()
			tmpRow.CreateDate = time.Unix(int64(res.OTimestamp())/1000000, 0)
			res.NextItem()
			tmpRow.UpdateDate = time.Unix(int64(res.OTimestamp())/1000000, 0)
			result = append(result, tmpRow)
		}
	}
	return &result, nil
}
