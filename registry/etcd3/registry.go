package etcd

import (
	"context"
	"encoding/json"
	etcd3 "github.com/coreos/etcd/clientv3"
	"google.golang.org/grpc/grpclog"
	"time"
)

const heart_time = 10

type EtcdReigistry struct {
	etcd3Client *etcd3.Client
	key         string
	value       string
	ID          etcd3.LeaseID
	ttl         time.Duration
	ctx         context.Context
	cancel      context.CancelFunc
}

type Option struct {
	EtcdConfig  etcd3.Config
	RegistryDir string
	ServiceName string
	NodeID      string
	NData       NodeData
}

type NodeData struct {
	Addr     string
	Metadata map[string]string
}

func NewRegistry(option Option) (*EtcdReigistry, error) {
	client, err := etcd3.New(option.EtcdConfig)
	if err != nil {
		return nil, err
	}

	val, err := json.Marshal(option.NData)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	//ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	registry := &EtcdReigistry{
		etcd3Client: client,
		key:         option.RegistryDir + "/" + option.ServiceName + "/" + option.NodeID,
		value:       string(val),
		ttl:         heart_time,
		ctx:         ctx,
		cancel:      cancel,
	}
	return registry, nil
}

func (e *EtcdReigistry) Register() error {

	insertFunc := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if e.ID == 0 {
			if resp, err := e.etcd3Client.Grant(ctx, int64(heart_time)); err != nil {
				return err
			} else {
				e.ID = resp.ID
				if _, err := e.etcd3Client.Put(ctx, e.key, e.value, etcd3.WithLease(e.ID)); err != nil {
					grpclog.Printf("grpclb: set key '%s' with ttl to etcd3 failed: %s", e.key, err.Error())
				} else {
					grpclog.Printf("grpclb: register fail")
				}
			}
		}
		if _, err := e.etcd3Client.KeepAlive(context.TODO(), e.ID); err != nil {
			grpclog.Printf("grpclb: key '%s' KeepAlive failed: %s", e.key, err.Error())
		}
		return nil
	}
	err := insertFunc()
	if err != nil {
		grpclog.Printf("grpclb: insertFunc failed: %s", e.key, err.Error())
	}
	ticker := time.NewTicker((e.ttl/2 - 1) * time.Second)
	for {
		select {
		case <-ticker.C:
			err = insertFunc()
			if err != nil {
				grpclog.Println(err)
			}
		case <-e.ctx.Done():
			ticker.Stop()
			if _, err := e.etcd3Client.Delete(context.Background(), e.key); err != nil {
				grpclog.Printf("grpclb: deregister '%s' failed: %s", e.key, err.Error())
			}
			return nil
		}
	}

	return nil
}

func (e *EtcdReigistry) Deregister() error {
	e.cancel()
	return nil
}
