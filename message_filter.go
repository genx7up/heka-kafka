package gman_plugins

import (
        "errors"
        "fmt"
        "github.com/mozilla-services/heka/pipeline"
        "github.com/mozilla-services/heka/message"
)

type MessageFilter struct {
        filter_by  string // Use Message_Matcher syntax to filter out messages
        output string // Target output plugin - next stop in pipeline
}

// Extract values from config and store it on the plugin instance.
func (f *MessageFilter) Init(config interface{}) error {
        var (
                filter_byConf interface{}
                outputConf interface{}
                ok         bool
        )

        conf := config.(pipeline.PluginConfig)
        if filter_byConf, ok = conf["filter_by"]; !ok {
                return errors.New("No 'filter_by' setting specified.")
        }
        if f.filter_by, ok = filter_byConf.(string); !ok {
                return errors.New("'filter_by' setting not a string value.")
        }
        if outputConf, ok = conf["output"]; !ok {
                return errors.New("No 'output' setting specified.")
        }
        if f.output, ok = outputConf.(string); !ok {
                return errors.New("'output' setting not a string value.")
        }
        return nil
}

func (f *MessageFilter) Run(runner pipeline.FilterRunner, helper pipeline.PluginHelper) (
        err error) {
        var (
                output   pipeline.OutputRunner
                ok       bool
        )
        if output, ok = helper.Output(f.output); !ok {
                return fmt.Errorf("No output: %s", output)
        }
        for pack := range runner.InChan() {
                ms, err := message.CreateMatcherSpecification(f.filter_by)
                if err != nil {
                        fmt.Println(err)
                        runner.LogError(err)
                        break
                }
                match := ms.Match(pack.Message)
                if match {
                        output.InChan() <- pack
                } else {
                        pack.Recycle(nil)
                }
        }
        return nil
}

func init() {
        pipeline.RegisterPlugin("MessageFilter", func() interface{} {
                return new(MessageFilter)
        })
}
