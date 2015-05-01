package gman_plugins

import (
        "encoding/json"
        "fmt"
        "github.com/mozilla-services/heka/pipeline"
)

type JsonEncoder struct {
}

func (re *JsonEncoder) Init(config interface{}) (err error) {
        return
}

func (re *JsonEncoder) Encode(pack *pipeline.PipelinePack) (output []byte, err error) {
        b, err := json.Marshal(pack.Message)
        if err != nil {
                fmt.Println("error: ", err)
        }
        return b, err
}

func init() {
        pipeline.RegisterPlugin("JsonEncoder", func() interface{} {
                return new(JsonEncoder)
        })
}
