package grpc

import (
	"IFS/pb"
	"net"
	"testing"
)

func Test(t *testing.T) {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		return
	}
	s := pb.RaftServiceServer()
}
