package writer

import (
	"context"
	"encoding/json"
	"git.tmaws.io/tracing/lightstep-tracer-go"
	"github.com/Jeffail/benthos/lib/processor"
	"github.com/Jeffail/benthos/lib/util/text"
	"github.com/opentracing/opentracing-go"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

type LightStepConfig struct {
	AccessToken string `json:"access_token" yaml:"access_token"`
	Hostname    string `json:"hostname" yaml:"hostname""`
	Port        int    `json:"port" yaml:"port"`
}

func NewLightStepConfig() LightStepConfig {
	return LightStepConfig{}
}

type LightStep struct {
	log   log.Modular
	stats metrics.Type
	conf  LightStepConfig

	connMut sync.RWMutex

	tracers map[string]lightstep.Tracer

	key   *text.InterpolatedBytes
	topic *text.InterpolatedString
}

func NewLightStep(conf LightStepConfig, log log.Modular, stats metrics.Type) (*LightStep, error) {
	ls := LightStep{
		conf:  conf,
		log:   log,
		stats: stats,
		tracers: make(map[string]lightstep.Tracer),
	}

	return &ls, nil
}

func (lse *LightStep) getOrMakeTracer(hostname, component string) (lightstep.Tracer, error) {
	lse.connMut.Lock()
	defer lse.connMut.Unlock()

	guid := strings.Join([]string{hostname,component}, ":")

	tags := opentracing.Tags{
		lightstep.ComponentNameKey: component,
		lightstep.HostnameKey:      hostname,
	}

	tracer, ok := lse.tracers[guid]
	if ok && tracer != nil {
		return tracer, nil
	}

	tracer = lightstep.NewTracer(lightstep.Options{
		AccessToken: lse.conf.AccessToken,
		Collector:   lightstep.Endpoint{Host: lse.conf.Hostname, Port: lse.conf.Port, Plaintext: true},
		UseGRPC:     true,
		Tags:        tags,
	})

	lse.tracers[guid] = tracer

	return tracer, nil
}

func (ls *LightStep) Connect() error {
	ls.connMut.Lock()
	defer ls.connMut.Unlock()
	ls.log.Infof("Sending traces to to address: %s\n", ls.conf.Hostname)
	return nil
}

type AtlLog struct {
	LogObj struct {
		Header struct {
		} `json:"header"`
	} `json:"log_obj"`
}

type LogObj struct {
	LogObj struct {
		Kvmap struct {
			Hostname    string `json:"hostname"`
			CommandType string `json:"command_type"`
			Command     struct {
				Message struct {
					Header struct {
						Mode     interface{} `json:"mode"`
						Sid      string      `json:"sid"`
						Token    float64     `json:"token"`
						Uid      string      `json:"uid"`
						Ver      string      `json:"ver"`
						Bid      string      `json:"bid"`
						Cip      string      `json:"cip"`
						Duration string      `json:"duration"`
					} `json:"header"`
				} `json:"message"`
				MessageId        float64     `json:"message_id"`
				SocketId         interface{} `json:"socket_id"`
				AltasClientAppIP string      `json:"altas_client_app_ip"`
			} `json:"command"`
		} `json:"kvmap"`
	} `json:"log_obj"`
	Timestamp float64 `json:"log_timestamp"`
}

func (ls *LightStep) Write(msg types.Message) error {

	err := msg.Iter(func(i int, p types.Part) error {
		var atl processor.AtlasLog

		err := json.Unmarshal(p.Get(), &atl)
		if err != nil {
			return err
		}

		if atl.Command.Unparsed != "" {
			return nil
		}

		resp := atl.Command.Response

		if resp.Message["header"] == nil {
			return nil
		}

		header := resp.Message["header"].(map[string]interface{})
		if header["duration"] == nil {
			return nil
		}

		spanOptions := []opentracing.StartSpanOption{
			opentracing.StartTime(atl.Timestamp),
		}

		d, err := time.ParseDuration(header["duration"].(string) + "s")
		if err != nil {
			return err
		}
		endTime := atl.Timestamp.Add(d)

		tracer, err := ls.getOrMakeTracer(atl.Hostname, "atlas")
		if err != nil {
			return types.ErrNotConnected
		}

		span := tracer.StartSpan(atl.CommandType, spanOptions...)
		span.SetTag("client", resp.Client.IP)

		for msg, v := range resp.Message {
			if msg == "header" {
				for k,val := range v.(map[string]interface{}) {
					if k == "duration" {
						continue
					}
					if k == "uid" {
						span.SetTag("guid:correlation_id", val)
						continue
					}
					if k == "sid" {
						span.SetTag("guid:sid", val)
						continue
					}
					span.SetTag(k, val)
				}
				continue
			}
			for k, val := range v.(map[string]interface{}) {
				switch val.(type) {
				case string:
					span.SetTag(msg + "." + k, val)
				case float64:
					span.SetTag(msg + "." + k, val)
				}
			}
		}

		span.FinishWithOptions(opentracing.FinishOptions{FinishTime: endTime})
		return nil
	})

	return err
}

func (ls *LightStep) CloseAsync() {
	ls.connMut.Lock()
	for _, tracer := range ls.tracers {
		if nil != tracer {
			lightstep.Flush(context.Background(), tracer)
			tracer = nil
		}
	}
	ls.connMut.Unlock()
}

func (ls *LightStep) WaitForClose(timeout time.Duration) error {
	return nil
}
