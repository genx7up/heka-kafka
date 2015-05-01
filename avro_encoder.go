package gman_plugins

import (
        "bytes"
        "fmt"
        "github.com/linkedin/goavro"
        "github.com/mozilla-services/heka/pipeline"
)

type AvroEncoder struct {
        recordSchemaJSON string
}

func (re *AvroEncoder) Init(config interface{}) (err error) {
        re.recordSchemaJSON = `
{
  "type": "record",
  "name": "comments",
  "doc:": "A basic schema for storing blog comments",
  "namespace": "com.example",
  "fields": [
    {
      "doc": "Name of user",
      "type": "string",
      "name": "username"
    },
    {
      "doc": "The content of the user's message",
      "type": "string",
      "name": "comment"
    },
    {
      "doc": "Unix epoch time in milliseconds",
      "type": "long",
      "name": "timestamp"
    }
  ]
}
`
        return
}

func (re *AvroEncoder) Encode(pack *pipeline.PipelinePack) (output []byte, err error) {

        someRecord, err := goavro.NewRecord(goavro.RecordSchema(re.recordSchemaJSON))
        if err != nil {
                fmt.Println("error: ", err)
        }

                fmt.Println("OK1")
        someRecord.Set("username", "Aquaman")
                fmt.Println("OK2")
        someRecord.Set("comment", "The Atlantic is oddly cold this morning!")
        // you can fully qualify the field name
        someRecord.Set("com.example.timestamp", int64(1082196484))

                fmt.Println("OK3")
        codec, err := goavro.NewCodec(re.recordSchemaJSON)
        if err != nil {
                fmt.Println("error: ", err)
        }

                fmt.Println("OK4")
        bb := new(bytes.Buffer)
        if err = codec.Encode(bb, someRecord); err != nil {
                fmt.Println("error: ", err)
        }
                fmt.Println("OK5")
        return bb.Bytes(), err
}

func init() {
        pipeline.RegisterPlugin("AvroEncoder", func() interface{} {
                return new(AvroEncoder)
        })
}
