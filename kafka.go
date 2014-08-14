package kafka

import (
	"errors"
	"strings"
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
	err = ao.init()
	return
}

func (ao *KafkaOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	inChan := or.InChan()
	errChan := ao.producer.Errors()

	var pack *PipelinePack
	var msg *message.Message
	var topic string
	var payload string
	var key string
	
	ok := true
	for ok {
		select {
		case pack, ok = <-inChan:
			if !ok {
				break
			}

			msg = pack.Message
			payload = msg.GetPayload()
			// topic = ao.config.Topic
			topic = msg.GetType()
			key = ao.config.Id
			pack.Recycle()

			err = ao.producer.QueueMessage(topic,  sarama.StringEncoder(key), sarama.ByteEncoder(payload))
			if err != nil {
				or.LogError(err)
			}
			break

		case err = <-errChan:
			break
		}
	}
	return
}

func (ao *KafkaOutput) CleanupForRestart() {
	ao.client.Close()
	ao.producer.Close()
	ao.init()
}

func (ao *KafkaOutput) init() (err error) {
	cconf := sarama.NewClientConfig()
	ao.client, err = sarama.NewClient(ao.config.Id, ao.addrs, cconf)
	if err != nil {
		return
	}
	kconf := sarama.NewProducerConfig()
	kconf.Partitioner = sarama.NewHashPartitioner()
	ao.producer, err = sarama.NewProducer(ao.client, kconf)
	if err != nil {
		return
	}
	return
}

func init() {
	RegisterPlugin("KafkaOutput", func() interface{} {
		return new(KafkaOutput)
	})
}
