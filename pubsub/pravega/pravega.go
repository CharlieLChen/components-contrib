/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pravega

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	client "github.com/pravega/pravega-client-rust/golang/pkg"
)

type Pvg struct {
	logger      logger.Logger
	scope       string
	stream      string
	url         string
	manager     *client.StreamManager
	writer      *client.StreamWriter
	readerGroup *client.StreamReaderGroup
	reader      *client.StreamReader
}

func NewPravega(log logger.Logger) pubsub.PubSub {
	return &Pvg{
		logger: log,
	}

}

func (pvg *Pvg) Close() error {
	if pvg.writer != nil {
		pvg.writer.Close()
	}
	if pvg.readerGroup != nil {
		pvg.readerGroup.Close()
	}
	if pvg.reader != nil {
		pvg.writer.Close()
	}
	if pvg.manager != nil {
		pvg.manager.Close()
	}

	pvg.logger.Infof("Reourses Close")
	return nil
}

func (pvg *Pvg) Features() []pubsub.Feature {
	return nil
}
func (pvg *Pvg) createStream() error {
	_, err := pvg.manager.CreateScope(pvg.scope)
	if err != nil {
		log.Fatalf("failed to create scope: %v", err)
		return err
	}

	streamConfig := client.NewStreamConfiguration(pvg.scope, pvg.stream)
	streamConfig.Scale.MinSegments = 1

	_, err = pvg.manager.CreateStream(streamConfig)
	if err != nil {
		log.Fatalf("failed to create stream: %v", err)
		return err
	}
	return nil
}
func (pvg *Pvg) Init(metadata pubsub.Metadata) error {
	pvg.logger.Infof("Init Properties: %v", metadata.Properties)
	scope, ok := metadata.Properties["scope"]
	if !ok {
		return fmt.Errorf("scope not found")
	}
	stream, ok := metadata.Properties["stream"]
	if !ok {
		return fmt.Errorf("stream not found")
	}
	url, ok := metadata.Properties["url"]
	if !ok {
		url = "127.0.0.1:9090"
	}
	pvg.scope = scope
	pvg.stream = stream
	pvg.url = url
	pvg.logger.Infof("scope: %v", pvg.scope)
	pvg.logger.Infof("stream: %v", pvg.stream)
	pvg.logger.Infof("url: %v", pvg.url)

	config := client.NewClientConfig()
	config.ControllerUri = pvg.url
	manager, err := client.NewStreamManager(config)
	if err != nil {
		pvg.logger.Errorf("failed to create sm: %v", err)
		return err
	}
	pvg.manager = manager

	pvg.logger.Infof("stream manager created")

	err = pvg.createStream()
	if err != nil {
		pvg.logger.Errorf("failed to create stream: %v", err)
		return err
	}
	pvg.logger.Infof("stream and scope created")

	return nil
}

func (pvg *Pvg) Publish(req *pubsub.PublishRequest) error {
	if pvg.writer == nil {
		writer, err := pvg.manager.CreateWriter(pvg.scope, pvg.stream)
		if err != nil {
			pvg.logger.Errorf("failed to create stream writer: %v", err)
			return err
		}
		pvg.logger.Infof("stream writer created")
		pvg.writer = writer
	}

	pvg.writer.WriteEvent(req.Data)
	err := pvg.writer.Flush()
	if err != nil {
		pvg.logger.Errorf("failed to write event:", err.Error())
		return err
	}
	return nil
}

func (pvg *Pvg) readData(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) {
	unixMilli := time.Now().UnixMilli()
	rgName := fmt.Sprintf("rg%d", unixMilli)
	rg, err := pvg.manager.CreateReaderGroup(rgName, pvg.scope, pvg.stream, false)
	if err != nil {
		pvg.logger.Errorf("failed to create reader group: %v", err.Error())
		return
	}
	pvg.readerGroup = rg
	pvg.logger.Infof("reader group %v created", rgName)
	reader, err := rg.CreateReader("reader1")
	if err != nil {
		pvg.logger.Errorf("failed to create reader: %v", err.Error())
		return
	}
	pvg.reader = reader
	pvg.logger.Infof("stream reader created")

	for {
		slice, err := pvg.reader.GetSegmentSlice()
		if err != nil {
			pvg.logger.Errorf("failed to get segment slice:", err.Error())
			return
		}

		for {
			event, err := slice.Next()
			if err != nil {
				pvg.logger.Errorf("failed to read event:", err.Error())
				return
			}
			if event != nil {
				// pvg.logger.Infof("event: %v", string(event))
				handler(ctx, &pubsub.NewMessage{
					Topic:    req.Topic,
					Data:     event,
					Metadata: req.Metadata,
				})
			} else {
				break
			}
		}
		slice.Close()
	}

}

func (pvg *Pvg) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	pvg.logger.Infof("Subscribe")
	go func() {
		pvg.readData(ctx, req, handler)
	}()

	return nil
}

/**
time.Sleep(time.Second)
		i := 0
		for {

			handler(ctx, &pubsub.NewMessage{
				Topic: req.Topic,
				// Data:        []byte("charlie"),
				// Data:     []byte("{\"data\":\"{\\\"orderId\\\":10}\",\"datacontenttype\":\"text/plain\",\"id\":\"c0edee5c-9f0b-4555-a9aa-c6245cb7c999\",\"pubsubname\":\"orderpubsub\",\"source\":\"checkout\",\"specversion\":\"1.0\",\"topic\":\"orders\",\"traceid\":\"00-f242c20704dc0487a1233b03a31462e7-58af50ee9774ca27-01\",\"traceparent\":\"00-f242c20704dc0487a1233b03a31462e7-58af50ee9774ca27-01\",\"tracestate\":\"\",\"type\":\"com.dapr.event.sent\"}"),
				Metadata: req.Metadata,
			})
			i++
			if i > 10 {
				break
			}
		}
*/
