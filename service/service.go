package service

import (
	log "github.com/Deansquirrel/goToolLog"
	"github.com/Deansquirrel/goYHChuand/global"
	"github.com/Deansquirrel/goYHChuand/worker"
	"time"
)

//启动服务内容
func StartService() error {
	log.Debug("StartService")
	go startUpdateRowsDate()
	time.Sleep(time.Second * 15)
	go startUpdateMdYyStateDate()
	return nil
}

func startUpdateRowsDate() {
	for {
		worker.UpdateRowsDate()
		time.Sleep(time.Second * global.DateUpdateDuration)
	}
}

func startUpdateMdYyStateDate() {
	worker.UpdateMdYyStateDate()
	time.Sleep(time.Second * global.DateUpdateDuration)
}
