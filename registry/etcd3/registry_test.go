package etcd

import (
	etcd "github.com/coreos/etcd/clientv3"
	"testing"
)

func TestNewRegistry(t *testing.T) {
	etcdConfg := etcd.Config{
		Endpoints: []string{"http://127.0.0.1:32769"},
	}
	registry, err := NewRegistry(
		Option{
			EtcdConfig:  etcdConfg,
			RegistryDir: "/grpc-lb",
			ServiceName: "hx",
			NodeID:      "node1",
			NData: NodeData{
				Addr: "127.0.0.1:8080",
			},
			Ttl: 20, //s
		})
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	err = registry.Register()
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	//time.Sleep(20 * time.Second)
}
