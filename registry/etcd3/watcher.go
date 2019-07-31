package etcd

import (
	"context"
	"encoding/json"
	etcd3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/naming"
	"sync"
)

// EtcdWatcher is the implementation of grpc.naming.Watcher
type EtcdWatcher struct {
	key     string
	client  *etcd3.Client
	updates []*naming.Update
	sign    chan *naming.Update
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

func (w *EtcdWatcher) Close() {
	w.cancel()
	w.wg.Wait()
	close(w.sign)
}

func newEtcdWatcher(key string, cli *etcd3.Client) naming.Watcher {
	ctx, cancel := context.WithCancel(context.Background())
	w := &EtcdWatcher{
		key:     key,
		client:  cli,
		ctx:     ctx,
		updates: make([]*naming.Update, 0),
		sign:    make(chan *naming.Update, 10),
		cancel:  cancel,
	}
	w.wg.Add(1)
	go w.watch()
	return w
}

func (w *EtcdWatcher) watch() {
	// generate etcd Watcher
	rch := w.client.Watch(w.ctx, w.key, etcd3.WithPrefix())
	defer w.wg.Done()
	for {
		select {
		case wresp, ok := <-rch:
			if !ok {
				return
			}
			if wresp.Err() != nil {
				grpclog.Error(wresp.Err())
				return
			}
			for _, ev := range wresp.Events {
				switch ev.Type {
				case mvccpb.PUT:
					nodeData := NodeData{}
					err := json.Unmarshal([]byte(ev.Kv.Value), &nodeData)
					if err != nil {
						grpclog.Println("Parse node data error:", err)
						continue
					}
					//fmt.Println("add:",nodeData)
					w.sign <- &naming.Update{Op: naming.Add, Addr: nodeData.Addr, Metadata: &nodeData.Metadata}
					//updates = append(updates, &naming.Update{Op: naming.Add, Addr: nodeData.Addr, Metadata: &nodeData.Metadata})
				case mvccpb.DELETE:
					//fmt.Printf("value:%+v",ev.Kv.Value)
					nodeData := NodeData{}
					err := json.Unmarshal([]byte(ev.Kv.Value), &nodeData)
					if err != nil {
						grpclog.Println("Parse node data error:", err)
						continue
					}
					//fmt.Println("delete:",nodeData)
					w.sign <- &naming.Update{Op: naming.Delete, Addr: nodeData.Addr, Metadata: &nodeData.Metadata}
					//updates = append(updates, &naming.Update{Op: naming.Delete, Addr: nodeData.Addr, Metadata: &nodeData.Metadata})
				}
			}
		case <-w.ctx.Done():
			return
		}
	}
}
func (w *EtcdWatcher) Next() ([]*naming.Update, error) {
	updates := make([]*naming.Update, 0)

	if len(w.updates) == 0 {
		// query addresses from etcd
		resp, err := w.client.Get(w.ctx, w.key, etcd3.WithPrefix())
		if err == nil {
			addrs := extractAddrs(resp)
			if len(addrs) > 0 {
				for _, addr := range addrs {
					v := addr
					updates = append(updates, &naming.Update{Op: naming.Add, Addr: v.Addr, Metadata: &v.Metadata})
				}
				w.updates = updates
				return updates, nil
			}
		} else {
			grpclog.Errorf("Etcd Watcher Get key error:", err)
		}
	}
	select {
	case addr, ok := <-w.sign:
		if !ok {
			return updates, nil
		}
		w.updates = append(w.updates, addr)
	case <-w.ctx.Done():
	}
	return w.updates, nil
}

func extractAddrs(resp *etcd3.GetResponse) []NodeData {
	addrs := []NodeData{}

	if resp == nil || resp.Kvs == nil {
		return addrs
	}

	for i := range resp.Kvs {
		if v := resp.Kvs[i].Value; v != nil {
			nodeData := NodeData{}
			err := json.Unmarshal(v, &nodeData)
			if err != nil {
				grpclog.Println("Parse node data error:", err)
				continue
			}
			addrs = append(addrs, nodeData)
		}
	}

	return addrs
}
