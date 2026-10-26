package plugin

import (
	"context"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

func (ds *MQTTDatasource) RunStream(ctx context.Context, req *backend.RunStreamRequest, sender *backend.StreamSender) error {
	// Extract the topic key from the channel path
	// Channel path format: "ds/{uid}/{topicKey}" where topicKey includes streaming key
	// We need to remove the channelPrefix ("ds/{uid}") to get the topic key
	topicKey := strings.TrimPrefix(req.Path, ds.channelPrefix+"/")
	logger := log.DefaultLogger.FromContext(ctx)

	chunks := strings.Split(topicKey, "/")

	// Expected format: {interval}/{encodedTopic}/{dsUid}/{hash}/{orgId}/{refId}
	if len(chunks) < 6 {
		return backend.DownstreamErrorf("invalid topic key: %s", topicKey)
	}

	refID := chunks[len(chunks)-1]

	interval, err := time.ParseDuration(chunks[0])
	if err != nil {
		return backend.DownstreamErrorf("invalid interval: %s", chunks[0])
	}

	_, err = ds.Client.Subscribe(topicKey, logger)
	if err != nil {
		return err
	}
	defer func() {
		if unsubErr := ds.Client.Unsubscribe(topicKey, logger); unsubErr != nil {
			logger.Error("Failed to unsubscribe from MQTT topic", "topicKey", topicKey, "error", unsubErr)
		}
	}()

	sendFrame := func(skipEmpty bool) {
		topic, ok := ds.Client.GetTopic(topicKey)
		if !ok {
			logger.Debug("topic not found", "path", req.Path, "topicKey", topicKey)
			return
		}
		frame, err := topic.ToDataFrame(logger)
		if err != nil {
			logger.Error("failed to convert topic to data frame", "path", req.Path, "error", backend.DownstreamError(err))
			return
		}
		if skipEmpty && frame.Rows() == 0 {
			return
		}
		frame.Name = refID
		frame = frame.SetRefID(refID)
		topic.KeepLastMessage()
		if err := sender.SendFrame(frame, data.IncludeAll); err != nil {
			logger.Error("failed to send data frame", "path", req.Path, "error", backend.DownstreamError(err))
		}
	}

	// Push retained/latest messages seeded by Subscribe right away instead of
	// waiting a full interval for the first frame.
	sendFrame(true)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Debug("stopped streaming (context canceled)", "path", req.Path, "topicKey", topicKey)
			return nil
		case <-ticker.C:
			sendFrame(false)
		}
	}
}

func (ds *MQTTDatasource) SubscribeStream(ctx context.Context, req *backend.SubscribeStreamRequest) (*backend.SubscribeStreamResponse, error) {
	// Extract namespace from the streaming key embedded in the channel path
	// Channel: {interval}/{topic}/{datasourceUid}/{hash}/{namespace}/{refId}
	pathParts := strings.Split(req.Path, "/")
	if len(pathParts) < 6 {
		return &backend.SubscribeStreamResponse{
			Status: backend.SubscribeStreamStatusNotFound,
		}, backend.DownstreamErrorf("invalid channel path format")
	}

	namespace := pathParts[len(pathParts)-2]
	pluginCfg := backend.PluginConfigFromContext(ctx)
	if namespace != pluginCfg.Namespace {
		return &backend.SubscribeStreamResponse{
			Status: backend.SubscribeStreamStatusPermissionDenied,
		}, backend.DownstreamErrorf("invalid namespace supplied in request")
	}

	return &backend.SubscribeStreamResponse{
		Status: backend.SubscribeStreamStatusOK,
	}, nil
}

func (ds *MQTTDatasource) PublishStream(_ context.Context, _ *backend.PublishStreamRequest) (*backend.PublishStreamResponse, error) {
	return &backend.PublishStreamResponse{
		Status: backend.PublishStreamStatusPermissionDenied,
	}, nil
}
