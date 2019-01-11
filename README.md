# Clickhouse batch saver
[![Build Status](https://travis-ci.org/nikonm/clkhsaver.svg?branch=master)](https://travis-ci.org/nikonm/clkhsaver)

Library for saving batch data in clickhouse, with dumping data if clickhouse unavailable

### Usage
```golang
// EmergencyDumper - used for save dump of data when clickhouse unavailable, 
// if then clickhouse is available all data restore to clickhouse queue
//Sample for Filesystem driver
dumperFileSystem := &EmergencyDumper{
	CheckInterval:time.Duration(5)*time.Second,
	Options: map[string]interface{}{"Type": "fs", "FS.Dir": "/tmp"},
}
//Sample for S3 driver
dumperForS3 := &EmergencyDumper{
	CheckInterval:time.Duration(5)*time.Second,
	Options: map[string]interface{}{
	    "Type": "s3",
	    "S3.Bucket": "test-bucket",
	    "S3.Endpoint": "minio.domain.local", 
	    "S3.AccessKey": "AccessKeySample",
	    "S3.SecretAccess": "SecretAccessSample",
	    "S3.UseSSL": true,
	    },
}

clckHouse := New(
	"Test",
	"TestUrl",
	time.Duration(10)*time.Second,
	time.Duration(5)*time.Second,
	5,
	func(err error) {
		t.Log(err)
	},
	dumperFileSystem,
)
err := clckHouse.Connect()
if err != nil {
    fmt.Printf("Message: %s\n", err.Error())
}
go clckHouse.Listener()


// Add row in queue for saving in clickhouse
clckHouse.Push(map[string]interface{}{"fields": "values"})

```