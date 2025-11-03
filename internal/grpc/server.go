package grpcserver

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/chahatsagarmain/GoStream/internal/store"
	pb "github.com/chahatsagarmain/GoStream/proto"
	"google.golang.org/grpc"
)

// grpcServer implements both ProducerService and ConsumerService.
type grpcServer struct {
	pb.UnimplementedProducerServiceServer
	pb.UnimplementedConsumerServiceServer
}

func (s *grpcServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	if err := store.CreateTopics(req.Topicname); err != nil {
		return &pb.CreateTopicResponse{Ok: false, Message: err.Error()}, nil
	}
	return &pb.CreateTopicResponse{Ok: true, Message: "topic created"}, nil
}

func (s *grpcServer) DeleteTopic(ctx context.Context, req *pb.DeleteTopicRequest) (*pb.DeleteTopicResponse, error) {
	if err := store.DeleteTopic(req.Topicname); err != nil {
		return &pb.DeleteTopicResponse{Ok: false, Message: err.Error()}, nil
	}
	return &pb.DeleteTopicResponse{Ok: true, Message: "deleted"}, nil
}

func (s *grpcServer) GetTopics(ctx context.Context, req *pb.GetTopicsRequest) (*pb.GetTopicsResponse, error) {
	topics, err := store.GetTopics()
	if err != nil {
		return nil, err
	}
	return &pb.GetTopicsResponse{Topics: topics}, nil
}

func (s *grpcServer) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	if err := store.AppendToLog(req.Topicname, req.Message); err != nil {
		return &pb.PublishResponse{Accepted: false, Message: err.Error()}, nil
	}
	return &pb.PublishResponse{Accepted: true, Message: "published"}, nil
}

func (s *grpcServer) CreateConsumer(ctx context.Context, req *pb.CreateConsumerRequest) (*pb.CreateConsumerResponse, error) {
	if err := store.CreateConsumer(req.Consumerid, req.Topicname); err != nil {
		return &pb.CreateConsumerResponse{Ok: false, Message: err.Error()}, nil
	}
	return &pb.CreateConsumerResponse{Ok: true, Message: "consumer created"}, nil
}

func (s *grpcServer) GetConsumers(ctx context.Context, req *pb.GetConsumersRequest) (*pb.GetConsumersResponse, error) {
	consumers, err := store.GetConsumers()
	if err != nil {
		return nil, err
	}
	return &pb.GetConsumersResponse{Consumers: consumers}, nil
}

func (s *grpcServer) GetOffset(ctx context.Context, req *pb.GetOffsetRequest) (*pb.GetOffsetResponse, error) {
	off, err := store.GetOffset(req.Consumerid, req.Topicname)
	if err != nil {
		return nil, err
	}
	return &pb.GetOffsetResponse{Offset: int32(off)}, nil
}

func (s *grpcServer) Fetch(ctx context.Context, req *pb.FetchRequest) (*pb.FetchResponse, error) {
	msg, err := store.GetMessageFromLog(req.Consumerid, req.Topicname)
	if err != nil {
		return nil, err
	}
	return &pb.FetchResponse{Message: msg}, nil
}

// StartGRPCServer starts a gRPC server on the provided address and registers services.
func StartGRPCServer(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s := grpc.NewServer()
	pb.RegisterProducerServiceServer(s, &grpcServer{})
	pb.RegisterConsumerServiceServer(s, &grpcServer{})
	log.Printf("gRPC server listening on %s", addr)
	return s.Serve(lis)
}
