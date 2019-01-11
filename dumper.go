package clkhsaver

import (
	"github.com/nikonm/clkhsaver/adapters"
	"time"
)

type EmergencyDumper struct {
	Logger        LoggerFunc
	CheckInterval time.Duration
	Options       map[string]interface{}
	ticker        *time.Ticker
	done          chan bool
	dataAdapter   adapters.DataAdapter
}

func (e *EmergencyDumper) Init() error {
	e.ticker = time.NewTicker(e.CheckInterval)
	e.done = make(chan bool)
	if e.Options["Type"] == "fs" {
		e.initialize(&adapters.FsAdapter{})
	} else {
		e.initialize(&adapters.S3Adapter{})
	}

	err := e.dataAdapter.Init(e.Options)
	if err != nil {
		return err
	}
	return nil
}

func (e *EmergencyDumper) initialize(adapter adapters.DataAdapter) {
	e.dataAdapter = adapter
}

func (e *EmergencyDumper) Watch(restoreCallback adapters.RestoreCallback) {
	for {
		select {
		case <-e.ticker.C:
			err := e.dataAdapter.Restore(restoreCallback)
			if err != nil {
				e.Logger(err)
				return
			}
			break
		case <-e.done:
			e.ticker.Stop()
			return
		}
	}
}

func (e *EmergencyDumper) Stop() {
	e.done <- true
}

func (e *EmergencyDumper) Dump(table string, entities []FieldsValues) {
	err := e.dataAdapter.Write(table, entities)
	if err != nil {
		e.Logger(err)
	}
}
