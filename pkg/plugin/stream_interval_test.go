package plugin

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/mqtt-datasource/pkg/mqtt"
)

func TestStreamInterval(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want time.Duration
	}{
		{0, time.Second},
		{-time.Second, time.Second},
		{100 * time.Millisecond, 100 * time.Millisecond},
		{time.Second, time.Second},
		{20 * time.Second, time.Second},
		{10 * time.Minute, time.Second},
	}
	for _, tt := range tests {
		if got := streamInterval(tt.in); got != tt.want {
			t.Errorf("streamInterval(%v) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestQueryChannelStableForLargeIntervals(t *testing.T) {
	ds := &MQTTDatasource{channelPrefix: "ds/test-uid"}
	queryJSON, _ := json.Marshal(map[string]interface{}{
		"topic":        "sensor/temperature",
		"streamingKey": "uid/hash/org/A",
	})

	channel := func(interval time.Duration) string {
		resp := ds.query(backend.DataQuery{JSON: queryJSON, Interval: interval, RefID: "A"})
		if resp.Error != nil {
			t.Fatalf("query failed: %v", resp.Error)
		}
		return resp.Frames[0].Meta.Channel
	}

	base := channel(time.Second)
	if !strings.HasPrefix(base, "ds/test-uid/1s/") {
		t.Fatalf("unexpected channel %q", base)
	}
	for _, interval := range []time.Duration{2 * time.Second, time.Minute, 30 * time.Minute} {
		if got := channel(interval); got != base {
			t.Errorf("channel for interval %v = %q, want %q", interval, got, base)
		}
	}
}

type recordingPacketSender struct {
	packets chan *backend.StreamPacket
}

func (r *recordingPacketSender) Send(p *backend.StreamPacket) error {
	r.packets <- p
	return nil
}

func TestRunStreamSendsSeededFrameImmediately(t *testing.T) {
	topicKey := "1h/dGVzdC90b3BpYw/ds-uid/hash/org/A"
	client := &mockMQTTClient{
		topics: map[string]*mqtt.Topic{
			topicKey: {
				Path:     "dGVzdC90b3BpYw",
				Interval: time.Hour,
				Messages: []mqtt.Message{{Topic: "test/topic", Timestamp: time.Now(), Value: []byte("42")}},
			},
		},
		subscriptions: map[string]bool{},
	}
	ds := &MQTTDatasource{Client: client, channelPrefix: "ds/test-uid"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sender := &recordingPacketSender{packets: make(chan *backend.StreamPacket, 1)}
	done := make(chan error, 1)
	go func() {
		done <- ds.RunStream(ctx, &backend.RunStreamRequest{Path: "ds/test-uid/" + topicKey}, backend.NewStreamSender(sender))
	}()

	select {
	case <-sender.packets:
	case <-time.After(2 * time.Second):
		t.Fatal("expected the seeded frame before the first interval tick")
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("RunStream returned error: %v", err)
	}
}

func TestRunStreamSkipsEmptyInitialFrame(t *testing.T) {
	topicKey := "1h/dGVzdC90b3BpYw/ds-uid/hash/org/A"
	client := &mockMQTTClient{topics: map[string]*mqtt.Topic{}, subscriptions: map[string]bool{}}
	ds := &MQTTDatasource{Client: client, channelPrefix: "ds/test-uid"}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	sender := &recordingPacketSender{packets: make(chan *backend.StreamPacket, 1)}
	if err := ds.RunStream(ctx, &backend.RunStreamRequest{Path: "ds/test-uid/" + topicKey}, backend.NewStreamSender(sender)); err != nil {
		t.Fatalf("RunStream returned error: %v", err)
	}
	select {
	case <-sender.packets:
		t.Fatal("expected no frame for a topic without messages")
	default:
	}
}
