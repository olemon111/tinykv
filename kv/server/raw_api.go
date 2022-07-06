package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	val, _ := reader.GetCF(req.GetCf(), req.GetKey())
	resp := &kvrpcpb.RawGetResponse{Value: val, NotFound: val == nil}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.GetKey(),
				Value: req.GetValue(),
				Cf:    req.GetCf(),
			},
		},
	}
	return &kvrpcpb.RawPutResponse{}, server.storage.Write(req.GetContext(), batch)
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.GetKey(),
				Cf:  req.GetCf(),
			},
		},
	}
	return &kvrpcpb.RawDeleteResponse{}, server.storage.Write(req.GetContext(), batch)
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.GetCf())
	cnt := uint32(0)
	var kvs []*kvrpcpb.KvPair
	// scan from startKey within limits
	for iter.Seek(req.GetStartKey()); iter.Valid() && cnt < req.GetLimit(); iter.Next() {
		item := iter.Item()
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, &kvrpcpb.KvPair{Key: item.Key(), Value: val})
		cnt++
	}
	iter.Close()
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
