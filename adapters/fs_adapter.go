package adapters

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

type FsAdapter struct {
	*Adapter
	dir string
}

func (a *FsAdapter) Init(options map[string]interface{}) error {
	if val, exist := options["FS.Dir"]; exist {
		a.dir = val.(string)
	}
	return nil
}

func (a *FsAdapter) Write(table string, entities []FieldsValues) error {
	var (
		err error
		f   *os.File
	)
	filename := fmt.Sprintf("%s-%d.bin", table, time.Now().Unix())
	_filepath := a.dir + `/` + filename
	f, err = os.Create(_filepath)

	if err != nil {
		return err
	}
	bytes, err := a.serialize(entities)
	if err != nil {
		return err
	}
	_, err = f.Write(bytes)

	if err != nil {
		return err
	}

	err = f.Sync()
	if err != nil {
		return err
	}
	err = f.Close()
	if err != nil {
		return err
	}
	if len(entities) < 1 {
		err = os.Remove(_filepath)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *FsAdapter) Restore(callback RestoreCallback) error {
	files, err := a.scan()
	if err != nil {
		return err
	}
	if len(files) > 0 {
		for table, file := range files {
			err = a.restore(table, file, callback)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *FsAdapter) restore(table, file string, callback RestoreCallback) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}

	defer f.Close()

	// Start reading from the file with a reader.
	reader := bufio.NewReader(f)
	data, err := ioutil.ReadAll(reader)
	queue, err := a.deserialize(data)
	if err != nil {
		return err
	}
	err = callback(table, queue)
	if err == nil {
		err = os.Remove(file)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *FsAdapter) scan() (map[string]string, error) {
	files := make(map[string]string)
	if _, err := os.Stat(a.dir); os.IsNotExist(err) {
		os.Mkdir(a.dir, 0755)
	}
	err := filepath.Walk(a.dir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if !info.IsDir() && filepath.Ext(info.Name()) == ".bin" {
				re, err := regexp.Compile("^(.*)\\-.*")
				if err != nil {
					return err
				}
				res := re.FindStringSubmatch(info.Name())
				if len(res) < 2 {
					return err
				}
				table := string(res[1])
				files[table] = a.dir + `/` + info.Name()
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	return files, nil
}
