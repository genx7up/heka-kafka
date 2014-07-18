package kafka

import (
	"bytes"
	"errors"
	"net/http"
	"strconv"
	"io/ioutil"
	"github.com/Shopify/sarama"
	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
)

type KafkaOutputConfig struct {
	Address string
	Id string
	Topic string
}

type KafkaOutput struct {
	config *KafkaOutputConfig
	addrs []string
	client *sarama.Client
	producer *sarama.Producer
}

func (ao *KafkaOutput) ConfigStruct() interface{} {
	return &KafkaOutputConfig{}
}

func (ao *KafkaOutput) Init(config interface{}) (err error) {
	ao.config = config.(*KafkaOutputConfig)
	ao.addrs = strings.Split(ao.config.Address, ",")
	if len(ao.addrs) == 1 && len(ao.addrs) == 0 {
		err = errors.New("invalid address")
	}
	ao.init()
	return
}

func (ao *KafkaOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	inChan := or.InChan()
	errChan := ao.producer.Errors()
	url := "http://" + ao.config.Address + "/put?topic=" + ao.config.Topic

	var pack *PipelinePack
	var msg *message.Message
	var body []byte
	var respData []byte
	var resp *http.Response

	ok := true
	for ok {
		select {
		case pack, ok = <-inChan:
			if !ok {
				break
			}
			msg = pack.Message
			err = ao.producer.QueueMessage(ao.config.Topic, sarama.ByteEncoder(msg.GetPayload()))
			if err != nil {
				pack.Recycle()
				or.LogError(err)
				break
			}
		}
		case err = <-errChan:
			break
	}
	return
}

func (ao *KafkaOutput) CleanupForRestart() {
	ao.client.Close()
	ao.producer.Close()
	init()
}

func (ao *KafkaOutput) init() {
	cconf = sarama.NewClientConfig()
	ao.client = sarama.NewClient(ao.config.Id, ao.addrs, cconf)
	kconf = sarama.NewProducerConfig()
	ao.producer = sarama.NewProducer(ao.client, kconf)
}

func init() {
	RegisterPlugin("KafkaOutput", func() interface{} {
		return new(KafkaOutput)
	})
}
