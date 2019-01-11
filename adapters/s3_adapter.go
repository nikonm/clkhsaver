package adapters

import (
	"bytes"
	"fmt"
	"github.com/minio/minio-go"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"time"
)

type S3Adapter struct {
	*Adapter
	client *minio.Client
	bucket string
}

func (a *S3Adapter) Init(options map[string]interface{}) error {
	var (
		err error
		endpoint string
		accessKey string
		secretAccess string
		useSSL = true
	)

	if a.client != nil {
		return nil
	}
	if val, exist := options["S3.Bucket"]; exist {
		a.bucket = val.(string)
	}

	if val, exist := options["S3.Endpoint"]; exist {
		endpoint = val.(string)
	}
	if val, exist := options["S3.AccessKey"]; exist {
		accessKey = val.(string)
	}
	if val, exist := options["S3.SecretAccess"]; exist {
		secretAccess = val.(string)
	}

	if val, exist := options["S3.UseSSL"]; exist {
		useSSL = val.(bool)
	}

	a.client, err = minio.New(endpoint, accessKey, secretAccess, useSSL)
	return err
}

func (a *S3Adapter) Write(symbol string, entities []FieldsValues) error {
	var err error

	filename := fmt.Sprintf("%s-%d.bin", symbol, time.Now().Unix())

	if err != nil {
		return err
	}
	_bytes, err := a.serialize(entities)
	if err != nil {
		return err
	}
	_, err = a.client.PutObject(
		a.bucket,
		filename,
		bytes.NewReader(_bytes),
		int64(len(_bytes)),
		minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		})

	if err != nil {
		return err
	}

	return nil
}

func (a *S3Adapter) scan() (map[string]string, error) {
	files := make(map[string]string)
	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{})
	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	objectCh := a.client.ListObjects(a.bucket, "", false, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			fmt.Println(object.Err)
		}

		if filepath.Ext(object.Key) == ".bin" {
			re, err := regexp.Compile("^(.*)\\-.*")
			if err != nil {
				return nil, err
			}
			res := re.FindStringSubmatch(object.Key)
			if len(res) < 2 {
				return nil, err
			}
			s := string(res[1])
			files[s] = object.Key
		}
	}
	return files, nil
}

func (a *S3Adapter) Restore(callback RestoreCallback) error {
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

func (a *S3Adapter) restore(symbol, file string, callback RestoreCallback) error {
	object, err := a.client.GetObject(a.bucket, file, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	data, err := ioutil.ReadAll(object)
	queue, err := a.deserialize(data)
	if err != nil {
		return err
	}
	err = callback(symbol, queue)
	if err == nil {
		err = a.client.RemoveObject(a.bucket, file)
		if err != nil {
			fmt.Println(err)
			return err
		}
	}
	return nil
}
