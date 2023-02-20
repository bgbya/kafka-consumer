package kafka_consumer_test

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"testing"
)

type AAAConsumerGroupHandler struct{}

func (AAAConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}
func (AAAConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// 这个方法用来消费消息的
func (h AAAConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// 获取消息
	for msg := range claim.Messages() {
		fmt.Printf("topic:%q partition:%d offset:%d value:%s\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		// 将消息标记为已使用
		sess.MarkMessage(msg, "")
	}
	return nil
}

func Test_kafka_consumer(t *testing.T) {
	config := sarama.NewConfig()
	group, err := sarama.NewConsumerGroup([]string{"**.**.76.120:9092"}, "AAA-group", config)

	if err != nil {
		fmt.Println("连接kafka失败：", err)
		return
	}
	go func() {
		for err := range group.Errors() {
			fmt.Println("分组错误 : ", err)
		}
	}()
	ctx := context.Background()
	fmt.Println("开始获取消息")
	// for 是应对 consumer rebalance
	for {
		// 需要监听的主题
		topics := []string{"test-topic"}
		handler := AAAConsumerGroupHandler{}
		// 启动kafka消费组模式，消费的逻辑在上面的 ConsumeClaim 这个方法里
		err := group.Consume(ctx, topics, handler)

		if err != nil {
			fmt.Println("消费失败; err : ", err)
			return
		}
	}
}

func consumer(brokenAddr []string, topic string, partition int32, offset int64) {
	consumer, err := sarama.NewConsumer(brokenAddr, nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer func() {
		if err = consumer.Close(); err != nil {
			fmt.Println(err)
			return
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)

	if err != nil {
		fmt.Println(err)
		return
	}

	defer func() {
		if err = partitionConsumer.Close(); err != nil {
			fmt.Println(err)
			return
		}
	}()

	for msg := range partitionConsumer.Messages() {
		fmt.Printf("partition:%d offset:%d key:%s val:%s\n", msg.Partition, msg.Offset, msg.Key, msg.Value)
	}
}
