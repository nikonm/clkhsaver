package clkhsaver

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type LoggerFunc = func(err error)

type FieldsValues = map[string]interface{}

type Entity struct {
	Data FieldsValues
}

//func (e *Entity) getFields() []string {
//	fields := make([]string, 0)
//	for f,_ := range e.Data {
//		fields = append(fields, f)
//	}
//	return fields
//}

type ClickHouseSaver struct {
	TableName         string
	ConnectionUrl     string
	PingInterval      time.Duration
	ReconnectInterval time.Duration
	BatchSize         int
	Logger            LoggerFunc
	Dumper            *EmergencyDumper

	connection  *sql.DB
	mutex       *sync.Mutex
	ticker      *time.Ticker
	done        chan bool
	entryCh     chan *Entity
	entityQueue []FieldsValues
}

func New(TableName, ConnectionUrl string, PingInterval, ReconnectInterval time.Duration, BatchSize int, Logger LoggerFunc, Dumper *EmergencyDumper) *ClickHouseSaver {

	return &ClickHouseSaver{
		TableName:         TableName,
		ConnectionUrl:     ConnectionUrl,
		PingInterval:      PingInterval,
		ReconnectInterval: ReconnectInterval,
		BatchSize:         BatchSize,
		Logger:            Logger,
		Dumper:            Dumper,
	}
}

func (c *ClickHouseSaver) Connect() error {
	var reconnect = c.connection != nil

	connection, err := sql.Open(`clickhouse`, c.ConnectionUrl)
	if err != nil {
		return err
	}

	if !reconnect {
		err = c.initialize(connection)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c * ClickHouseSaver) initialize(connection *sql.DB) error {
	err := c.Dumper.Init()
	if err != nil {
		return err
	}
	c.connection = connection
	c.ticker = time.NewTicker(c.PingInterval)
	c.done = make(chan bool)
	c.mutex = &sync.Mutex{}
	if c.entryCh == nil {
		c.entryCh = make(chan *Entity)
	}
	return nil
}

func (c *ClickHouseSaver) Listener() {
	go c.Dumper.Watch(c.SaveQueue)
	for {
		select {
		case e := <-c.entryCh:
			c.collectEntities(e)
			c.save(false)
			break
		case <-c.ticker.C:
			err := c.connection.Ping()
			if err != nil {
				time.Sleep(c.ReconnectInterval)
				c.Logger(c.Connect())
			}
			break
		case <-c.done:
			c.ticker.Stop()
			return
		}
	}
}

func (c *ClickHouseSaver) Reconnect() {
	err := c.connection.Close()
	if err != nil {
		c.Logger(err)
	}
	c.connection = nil
	c.Logger(c.Connect())
}

func (c *ClickHouseSaver) Close() {
	if c.connection == nil {
		return
	}
	c.done <- true
	c.save(true)
	err := c.connection.Close()
	c.connection = nil
	if err != nil {
		c.Logger(err)
	}
}

func (c *ClickHouseSaver) Push(data FieldsValues) {
	c.entryCh <- &Entity{Data: data}
}

func (c *ClickHouseSaver) collectEntities(entity *Entity) {
	c.entityQueue = append(c.entityQueue, entity.Data)
}

func (c *ClickHouseSaver) save(forced bool) {
	if len(c.entityQueue) >= c.BatchSize || forced {
		c.mutex.Lock()

		queue := make([]FieldsValues, len(c.entityQueue))
		copy(queue, c.entityQueue)
		c.entityQueue = nil

		c.mutex.Unlock()

		if forced {
			c.safetySaveQueue(queue)
		} else {
			go c.safetySaveQueue(queue)
		}
	}
}

func (c *ClickHouseSaver) safetySaveQueue(queue []FieldsValues) {
	err := c.SaveQueue(c.TableName, queue)
	if err != nil {
		c.Logger(err)
		c.Dumper.Dump(c.TableName, queue)
	}
}

func (c *ClickHouseSaver) SaveQueue(table string, queue []FieldsValues) error {
	tx, err := c.connection.Begin()
	if err != nil {
		c.Logger(errors.New("Transaction failed!"))
		return err
	}

	fields := "\""
	for f, _ := range queue[0] {
		fields += f + "\", "
	}
	columnsCount := len(queue[0])
	fields = strings.TrimRight(fields, ", ")
	_sql := `INSERT INTO ` + table + ` (` + fields + `) VALUES `
	_sql += "(" + strings.TrimRight(strings.Repeat("?,", columnsCount), ",") + ")"
	stmt, err := tx.Prepare(_sql)
	if err != nil {
		c.Logger(errors.New("Transaction prepare failed!"))
		return err
	}
	for _, values := range queue {
		_, err = stmt.Exec(mapToSlice(values)...)
		if err != nil {
			c.Logger(errors.New(fmt.Sprintf("Transaction exec failed! %s", err.Error())))
			return err
		}
	}
	err = tx.Commit()
	if err != nil {
		c.Logger(errors.New("Transaction commit failed!"))
	} else {
		c.Logger(errors.New(fmt.Sprintf("Inserted into '%s' count: %d", table, len(queue))))
	}

	return err
}

func mapToSlice(values FieldsValues) []interface{} {
	r := make([]interface{}, 0)
	for _, v := range values {
		r = append(r, v)
	}
	return r
}
