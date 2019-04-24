package worker

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/Deansquirrel/goToolCommon"
	"github.com/Deansquirrel/goToolMSSql"
	"github.com/Deansquirrel/goYHChuand/global"
	"time"
)

import log "github.com/Deansquirrel/goToolLog"

const (
	sqlGetRev1 = "select count(*) from tfrcv1"
	sqlGetRev2 = "select count(*) from tfrcv2"
	sqlGetRev3 = "select count(*) from tfrcv3"
	sqlGetRev4 = "select count(*) from tfrcv4"

	sqlGetMdYyState = "" +
		"select A.[mdyyid],A.[mdyydate],A.[mdyystatus],A.[mdyyopenid],A.[mdyyopentime],A.[mdyycloseid],A.[mdyyclosetime],A.[mdyysjtype] " +
		" from xtmdyystatus_md a" +
		" inner join (" +
		"	 	select * " +
		"		from ( " +
		"			select mdyyid,max(mdyydate) as mdyydate " +
		"			from xtmdyystatus_md group by mdyyid " +
		"		) a " +
		"		union all " +
		"		select mdyyid,max(mdyydate) as mdyydate " +
		"		from xtmdyystatus_md a " +
		"		where not exists ( " +
		"			select * " +
		"			from ( " +
		"				select mdyyid,max(mdyydate) as mdyydate " +
		"				from xtmdyystatus_md group by mdyyid " +
		"			) b " +
		"			where a.mdyyid = b.mdyyid and a.mdyydate = b.mdyydate " +
		"		) group by mdyyid " +
		"	) b on a.mdyyid = b.mdyyid and a.mdyydate = b.mdyydate"
)

const (
	sqlRcvDate = "" +
		"INSERT INTO [RcvState]([OprTime],[tfrcv1],[tfrcv2],[tfrcv3],[tfrcv4])" +
		" VALUES (getDate(),?,?,?,?)"

	sqlSaveMdYySate = "" +
		"INSERT INTO [xtmdyystatus_md]([mdyyid],[mdyydate],[mdyystatus],[mdyyopenid],[mdyyopentime],[mdyycloseid],[mdyyclosetime],[mdyysjtype])" +
		" VALUES (?,?,?,?,?,?,?,?)"
	sqlClearMdYySate = "" +
		"TRUNCATE TABLE [xtmdyystatus_md]"
)

const (
	saveDbServer = ""
	saveDbPort   = 1433
	saveDbDbName = ""
	saveDbUser   = ""
	saveDbPwd    = ""
)

//获取查询DB配置
func getSearchDBConfig() *goToolMSSql.MSSqlConfig {
	return &goToolMSSql.MSSqlConfig{
		Server: global.SysConfig.DB.Server,
		Port:   global.SysConfig.DB.Port,
		DbName: global.SysConfig.DB.DbName,
		User:   global.SysConfig.DB.User,
		Pwd:    global.SysConfig.DB.Pwd,
	}
}

//获取保存DB配置
func getSaveDBConfig() *goToolMSSql.MSSqlConfig {
	return &goToolMSSql.MSSqlConfig{
		Server: saveDbServer,
		Port:   saveDbPort,
		DbName: saveDbDbName,
		User:   saveDbUser,
		Pwd:    saveDbPwd,
	}
}

//查询数据
func getRowsBySQL(sql string, dbConfig *goToolMSSql.MSSqlConfig) (*sql.Rows, error) {
	conn, err := goToolMSSql.GetConn(dbConfig)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}
	rows, err := conn.Query(sql)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}
	return rows, nil
}

func UpdateMdYyStateDate() {
	log.Debug("start UpdateMdYyStateDate")
	defer log.Debug("UpdateMdYyStateDate Complete")
	conn, err := goToolMSSql.GetConn(getSaveDBConfig())
	if err != nil {
		log.Error(fmt.Sprintf("Get SaveDB Conn error：%s", err.Error()))
		return
	}

	_, err = conn.Exec(sqlClearMdYySate)
	if err != nil {
		log.Error(fmt.Sprintf("Clear MdYyState error：%s", err.Error()))
		return
	}

	rows, err := getRowsBySQL(sqlGetMdYyState, getSearchDBConfig())
	if err != nil {
		log.Error(fmt.Sprintf("Get MdYyState Data error：%s", err.Error()))
		return
	}
	defer func() {
		_ = rows.Close()
	}()
	list := make([]mdYyState, 0)
	for rows.Next() {
		d := mdYyState{}
		err = rows.Scan(&d.Id, &d.Date, &d.Status, &d.OpenId, &d.OpenTime, &d.CloseId, &d.CloseTime, &d.SjType)
		if err != nil {
			log.Error(fmt.Sprintf("Scan MdYyState error：%s", err.Error()))
		} else {
			list = append(list, d)
		}
	}
	for _, d := range list {
		saveMdYyStateData(&d)
	}
}

type mdYyState struct {
	Id        int
	Date      string
	Status    int
	OpenId    int
	OpenTime  time.Time
	CloseId   int
	CloseTime time.Time
	SjType    int
}

func UpdateRowsDate() {
	log.Debug("start UpdateRowsDate")
	defer log.Debug("UpdateRowsDate Complete")
	rcv1 := getRcv(sqlGetRev1)
	rcv2 := getRcv(sqlGetRev2)
	rcv3 := getRcv(sqlGetRev3)
	rcv4 := getRcv(sqlGetRev4)
	saveRcvData(rcv1, rcv2, rcv3, rcv4)
}

func getRcv(sql string) int {
	var rcv int
	rows, err := getRowsBySQL(sql, getSearchDBConfig())
	if err != nil {
		log.Error(fmt.Sprintf("Get Rcv Error: %s", err.Error()))
	} else {
		defer func() {
			_ = rows.Close()
		}()
		for rows.Next() {
			err = rows.Scan(&rcv)
			if err != nil {
				log.Error(fmt.Sprintf("Get Rcv Value Error: %s", err.Error()))
			}
		}
	}
	return rcv
}

func saveRcvData(rcv1, rcv2, rcv3, rcv4 int) {
	conn, err := goToolMSSql.GetConn(getSaveDBConfig())
	if err != nil {
		log.Error(fmt.Sprintf("Get SaveDB Conn error：%s,Data： %d %d %d %d", err.Error(), rcv1, rcv2, rcv3, rcv4))
		return
	}

	_, err = conn.Exec(sqlRcvDate, rcv1, rcv2, rcv3, rcv4)
	if err != nil {
		log.Error(fmt.Sprintf("Save RcvData error：%s,Data： %d %d %d %d", err.Error(), rcv1, rcv2, rcv3, rcv4))
		return
	}
}

func saveMdYyStateData(d *mdYyState) {
	conn, err := goToolMSSql.GetConn(getSaveDBConfig())
	if err != nil {
		log.Error(fmt.Sprintf("Get SaveDB Conn error：%s,", err.Error()))
		strData, err := json.Marshal(d)
		if err == nil {
			log.Error(fmt.Sprintf("Data：%s", strData))
		}
		return
	}

	_, err = conn.Exec(sqlSaveMdYySate, d.Id, d.Date, d.Status, d.OpenId, goToolCommon.GetDateTimeStr(d.OpenTime), d.CloseId, goToolCommon.GetDateTimeStr(d.CloseTime), d.SjType)
	if err != nil {
		log.Error(fmt.Sprintf("Save MdYyState error：%s,", err.Error()))
		strData, err := json.Marshal(d)
		if err == nil {
			log.Error(fmt.Sprintf("Data：%s", strData))
		}
		return
	}
}
