package service

import (
	log "github.com/Deansquirrel/goToolLog"
	"github.com/Deansquirrel/goYHChuand/global"
	"github.com/Deansquirrel/goYHChuand/worker"
	"github.com/robfig/cron"
)

//启动服务内容
func StartService() error {
	log.Debug("StartService")
	var err error
	c := cron.New()
	err = c.AddFunc(global.SysConfig.Task.RowDataUpdateCron, worker.UpdateRowsData)

	if err != nil {
		return err
	}

	err = c.AddFunc(global.SysConfig.Task.YyStateUpdateCron, worker.UpdateMdYyStateData)

	if err != nil {
		return err
	}
	c.Start()
	return nil
}
