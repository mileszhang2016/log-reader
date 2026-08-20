// Copyright (c) 2026 The BFE Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mod_kafka

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bfenetworks/go-lib/log"
	"github.com/bfenetworks/go-lib/web-monitor/module_state2"
	"github.com/segmentio/kafka-go"
)

// KafkaProducer Kafka 生产者封装
type KafkaProducer struct {
	conf      *ConfModKafka
	writer    *kafka.Writer
	dlqWriter *kafka.Writer
	msgCh     chan []byte
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	state *module_state2.State
}

// NewKafkaProducer 创建 KafkaProducer
func NewKafkaProducer(state *module_state2.State, conf *ConfModKafka) (*KafkaProducer, error) {
	brokers := strings.Split(conf.Kafka.Brokers, ",")
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no kafka brokers configured")
	}

	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	ctx, cancel := context.WithCancel(context.Background())

	kp := &KafkaProducer{
		conf:   conf,
		msgCh:  make(chan []byte, conf.Kafka.BatchSize*2),
		ctx:    ctx,
		cancel: cancel,
	}
	kp.state = state

	kp.writer = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        conf.Kafka.Topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    conf.Kafka.BatchSize,
		BatchTimeout: time.Duration(conf.Kafka.LingerMs) * time.Millisecond,
		MaxAttempts:  conf.Kafka.MaxRetries,
	}

	switch conf.Kafka.Compression {
	case "snappy":
		kp.writer.Compression = kafka.Snappy
	case "gzip":
		kp.writer.Compression = kafka.Gzip
	case "lz4":
		kp.writer.Compression = kafka.Lz4
	case "zstd":
		kp.writer.Compression = kafka.Zstd
	}

	if conf.Kafka.DeadLetterTopic != "" {
		kp.dlqWriter = &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    conf.Kafka.DeadLetterTopic,
			Balancer: &kafka.Hash{},
		}
	}

	return kp, nil
}

// Send 异步发送消息到 Kafka
func (kp *KafkaProducer) Send(msg []byte) {
	select {
	case kp.msgCh <- msg:
	default:
		kp.state.Inc("SENT_KAFKA_CHN_FULL", 1)
		log.Logger.Warn("mod_kafka: message channel full, dropping message")
	}
}

// Start 启动后台发送协程
func (kp *KafkaProducer) Start() {
	kp.wg.Add(1)
	go kp.sendLoop()
}

// sendLoop 后台发送循环
func (kp *KafkaProducer) sendLoop() {
	defer kp.wg.Done()

	var batch []kafka.Message
	ticker := time.NewTicker(time.Duration(kp.conf.Kafka.LingerMs) * time.Millisecond)
	defer ticker.Stop()

	flush := func(prompt string) {
		if len(batch) == 0 {
			return
		}

		ctx, cancel := context.WithTimeout(kp.ctx, 10*time.Second)
		defer cancel()

		if openDebug {
			log.Logger.Debug("flush to kafka. prompt:%s, batch size:%d", prompt, len(batch))
		}

		err := kp.writer.WriteMessages(ctx, batch...)
		if err != nil {
			kp.state.Inc("SEND_KAFKA_FAILED", len(batch))
			log.Logger.Error("mod_kafka: write messages failed: %v", err)
			kp.writeToDLQ(batch)
		}

		batch = batch[:0]
	}

	for {
		select {
		case <-kp.ctx.Done():
			for {
				select {
				case msg := <-kp.msgCh:
					batch = append(batch, kafka.Message{Value: msg})
				default:
					flush("end")
					return
				}
			}

		case msg := <-kp.msgCh:
			batch = append(batch, kafka.Message{Value: msg})
			if len(batch) >= kp.conf.Kafka.BatchSize {
				flush("batch")
			}

		case <-ticker.C:
			flush("timeout")
		}
	}
}

// writeToDLQ 写入死信队列
func (kp *KafkaProducer) writeToDLQ(messages []kafka.Message) {
	if kp.dlqWriter == nil {
		return
	}

	kp.state.Inc("DLQ_SENT", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := kp.dlqWriter.WriteMessages(ctx, messages...)
	if err != nil {
		kp.state.Inc("DLQ_SENT_FAILED", 1)
		log.Logger.Error("mod_kafka: write to DLQ failed: %v", err)
	}
}

// Close 关闭 KafkaProducer
func (kp *KafkaProducer) Close() error {
	kp.cancel()
	kp.wg.Wait()

	if kp.writer != nil {
		if err := kp.writer.Close(); err != nil {
			log.Logger.Error("mod_kafka: close writer failed: %v", err)
		}
	}

	if kp.dlqWriter != nil {
		if err := kp.dlqWriter.Close(); err != nil {
			log.Logger.Error("mod_kafka: close DLQ writer failed: %v", err)
		}
	}

	return nil
}
