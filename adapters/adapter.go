package adapters

import (
	"bytes"
	"encoding/gob"
	"errors"
)

type FieldsValues = map[string]interface{}

type DataAdapter interface {
	Write(symbol string, entities []FieldsValues) error
	Restore(callback RestoreCallback) error
	Init(options map[string]interface{}) error
	serialize(entities []FieldsValues) ([]byte, error)
	deserialize(data []byte) ([]FieldsValues, error)
}

type Adapter struct {
	Options map[string]interface{}
}

type RestoreCallback = func(table string, queue []FieldsValues) error

func (this *Adapter) Init(options map[string]interface{}) error {
	this.Options = options
	return nil
}

func (this *Adapter) Write(table string, entities []FieldsValues) error {
	return errors.New("Not implemented!")
}

func (this *Adapter) Restore(callback RestoreCallback) error {
	return errors.New("Not implemented!")
}

func (this *Adapter) serialize(entities []FieldsValues) ([]byte, error) {
	buf := bytes.Buffer{}
	_gob := gob.NewEncoder(&buf)
	err := _gob.Encode(entities)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (this *Adapter) deserialize(data []byte) ([]FieldsValues, error) {
	m := make([]FieldsValues, 0)

	b := bytes.Buffer{}
	b.Write(data)

	d := gob.NewDecoder(&b)
	err := d.Decode(&m)
	if err != nil {
		return nil, err
	}
	return m, nil
}
