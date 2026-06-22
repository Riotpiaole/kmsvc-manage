// Package handlers implements the kafkamgmt.v1 QueueServiceServer interface
// (design.md §2b) by delegating to internal/core/queue's business logic —
// this layer only translates between proto messages and that package's
// plain Go types, and maps domain errors to gRPC status codes.
package handlers

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	kafkamgmtv1 "forgejo.riotpiao.homelab.com/rock/kmsvc-proto/gen/kafkamgmt/v1"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/rockliang/kafka-management-service/internal/core/queue"
)

// QueueService implements kafkamgmtv1.QueueServiceServer.
type QueueService struct {
	kafkamgmtv1.UnimplementedQueueServiceServer

	Redis  *goredis.Client
	Router *queue.ShardRouter

	Send       *queue.SendMessageService
	Delete     *queue.DeleteMessageService
	Visibility *queue.ChangeVisibilityService
	Consumers  *ConsumerRegistry

	// ReceivePollInterval overrides the default poll interval used while
	// long-polling; primarily for tests. Zero uses the package default.
	ReceivePollInterval time.Duration
}

func (s *QueueService) SendMessage(ctx context.Context, req *kafkamgmtv1.SendMessageRequest) (*kafkamgmtv1.SendMessageResponse, error) {
	out, err := s.Send.SendMessage(ctx, queue.SendMessageInput{
		QueueName:              req.GetQueueName(),
		Body:                   string(req.GetMessageBody()),
		MessageGroupID:         req.GetMessageGroupId(),
		MessageDeduplicationID: req.GetMessageDeduplicationId(),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return &kafkamgmtv1.SendMessageResponse{
		MessageId:      out.MessageID,
		SequenceNumber: strconv.FormatInt(out.SequenceNumber, 10),
	}, nil
}

func (s *QueueService) SendMessageBatch(ctx context.Context, req *kafkamgmtv1.SendMessageBatchRequest) (*kafkamgmtv1.SendMessageBatchResponse, error) {
	resp := &kafkamgmtv1.SendMessageBatchResponse{}
	for _, entry := range req.GetEntries() {
		out, err := s.Send.SendMessage(ctx, queue.SendMessageInput{
			QueueName:              req.GetQueueName(),
			Body:                   string(entry.GetMessageBody()),
			MessageGroupID:         entry.GetMessageGroupId(),
			MessageDeduplicationID: entry.GetMessageDeduplicationId(),
		})
		if err != nil {
			resp.Failed = append(resp.Failed, &kafkamgmtv1.BatchResultEntry{Id: entry.GetId(), Error: err.Error()})
			continue
		}
		resp.Successful = append(resp.Successful, &kafkamgmtv1.BatchResultEntry{Id: entry.GetId(), MessageId: out.MessageID})
	}
	return resp, nil
}

func (s *QueueService) ReceiveMessage(ctx context.Context, req *kafkamgmtv1.ReceiveMessageRequest) (*kafkamgmtv1.ReceiveMessageResponse, error) {
	fetcher, err := s.Consumers.Get(ctx, req.GetQueueName())
	if err != nil {
		return nil, mapError(err)
	}
	// A fresh ReceiveMessageService per call: Redis/Router are shared and
	// safe for concurrent use, but Fetcher is request-scoped (each call may
	// target a different queue's consumer), so the service struct itself
	// must not be shared across concurrent requests.
	recv := &queue.ReceiveMessageService{
		Redis:        s.Redis,
		Fetcher:      fetcher,
		Router:       s.Router,
		PollInterval: s.ReceivePollInterval,
	}

	msgs, err := recv.ReceiveMessage(ctx, queue.ReceiveMessageInput{
		QueueName:                 req.GetQueueName(),
		MaxNumberOfMessages:       req.GetMaxNumberOfMessages(),
		WaitTime:                  time.Duration(req.GetWaitTimeSeconds()) * time.Second,
		VisibilityTimeoutOverride: time.Duration(req.GetVisibilityTimeoutSeconds()) * time.Second,
	})
	if err != nil {
		return nil, mapError(err)
	}

	out := make([]*kafkamgmtv1.Message, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, &kafkamgmtv1.Message{
			MessageId:     m.ReceiptHandle,
			ReceiptHandle: m.ReceiptHandle,
			Body:          []byte(m.Body),
			ReceiveCount:  m.ReceiveCount,
			EnqueuedAt:    timestamppb.Now(),
		})
	}
	return &kafkamgmtv1.ReceiveMessageResponse{Messages: out}, nil
}

func (s *QueueService) DeleteMessage(ctx context.Context, req *kafkamgmtv1.DeleteMessageRequest) (*kafkamgmtv1.DeleteMessageResponse, error) {
	if err := s.Delete.DeleteMessage(ctx, req.GetQueueName(), req.GetReceiptHandle()); err != nil {
		return nil, mapError(err)
	}
	return &kafkamgmtv1.DeleteMessageResponse{}, nil
}

func (s *QueueService) DeleteMessageBatch(ctx context.Context, req *kafkamgmtv1.DeleteMessageBatchRequest) (*kafkamgmtv1.DeleteMessageBatchResponse, error) {
	resp := &kafkamgmtv1.DeleteMessageBatchResponse{}
	for _, entry := range req.GetEntries() {
		if err := s.Delete.DeleteMessage(ctx, req.GetQueueName(), entry.GetReceiptHandle()); err != nil {
			resp.Failed = append(resp.Failed, &kafkamgmtv1.BatchResultEntry{Id: entry.GetId(), Error: err.Error()})
			continue
		}
		resp.Successful = append(resp.Successful, &kafkamgmtv1.BatchResultEntry{Id: entry.GetId()})
	}
	return resp, nil
}

func (s *QueueService) ChangeMessageVisibility(ctx context.Context, req *kafkamgmtv1.ChangeMessageVisibilityRequest) (*kafkamgmtv1.ChangeMessageVisibilityResponse, error) {
	timeout := time.Duration(req.GetVisibilityTimeoutSeconds()) * time.Second
	if err := s.Visibility.ChangeMessageVisibility(ctx, req.GetQueueName(), req.GetReceiptHandle(), timeout); err != nil {
		return nil, mapError(err)
	}
	return &kafkamgmtv1.ChangeMessageVisibilityResponse{}, nil
}

// mapError maps internal/core/queue's plain errors to gRPC status codes.
// These services return fmt.Errorf-wrapped strings rather than typed
// sentinels, so this matches on substring rather than errors.Is — a
// pragmatic v1 choice documented here rather than introducing typed errors
// purely for this translation layer.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "not found"):
		return status.Error(codes.NotFound, msg)
	case strings.Contains(msg, "exceeds") || strings.Contains(msg, "required for FIFO") || strings.Contains(msg, "no consumable shards") || strings.Contains(msg, "not reconciled"):
		return status.Error(codes.InvalidArgument, msg)
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, msg)
	default:
		return status.Error(codes.Internal, msg)
	}
}
