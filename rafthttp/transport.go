// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rafthttp

import (
	"net/http"
	"sync"
	"time"

	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/logutil"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/pkg/capnslog"
	"github.com/xiang90/probing"
	"golang.org/x/net/context"
)

var plog = logutil.NewMergeLogger(capnslog.NewPackageLogger("github.com/coreos/etcd", "rafthttp"))

// Raft 接口
type Raft interface {
	Process(ctx context.Context, m raftpb.Message) error
	IsIDRemoved(id uint64) bool
	ReportUnreachable(id uint64)
	ReportSnapshot(id uint64, status raft.SnapshotStatus)
}

// Transporter 接口
type Transporter interface {
	// Start starts the given Transporter.
	// Start MUST be called before calling other functions in the interface.
	// Start 启动给定的Transporter，Start必须在调用其他函数前调用
	Start() error
	// Handler returns the HTTP handler of the transporter.
	// A transporter HTTP handler handles the HTTP requests
	// from remote peers.
	// The handler MUST be used to handle RaftPrefix(/raft)
	// endpoint.
	// Handler 返回transporter的http handler
	Handler() http.Handler
	// Send sends out the given messages to the remote peers.
	// Each message has a To field, which is an id that maps
	// to an existing peer in the transport.
	// If the id cannot be found in the transport, the message
	// will be ignored.
	// Send 发送给定的消息到远端节点，每个消息里面的To field，都是transport中一个peer的id
	Send(m []raftpb.Message)
	// SendSnapshot sends out the given snapshot message to a remote peer.
	// The behavior of SendSnapshot is similar to Send.
	// SendSnapshot 将snapshot信息发送给远端peer
	SendSnapshot(m snap.Message)
	// AddRemote adds a remote with given peer urls into the transport.
	// A remote helps newly joined member to catch up the progress of cluster,
	// and will not be used after that.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	// AddRemote 添加一个给定的peer url的remote加入到该transport
	AddRemote(id types.ID, urls []string)
	// AddPeer adds a peer with given peer urls into the transport.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	// Peer urls are used to connect to the remote peer.
	// AddPeer添加一个给定的peer url的peer到该transport
	AddPeer(id types.ID, urls []string)
	// RemovePeer removes the peer with given id.
	// RemovePeer 去除给定id的peer
	RemovePeer(id types.ID)
	// RemoveAllPeers removes all the existing peers in the transport.
	// RemoveAllPeers 去除transport里的所有存在的peers
	RemoveAllPeers()
	// UpdatePeer updates the peer urls of the peer with the given id.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	// UpdatePeer 更新给定id的peer 的url
	UpdatePeer(id types.ID, urls []string)
	// ActiveSince returns the time that the connection with the peer
	// of the given id becomes active.
	// If the connection is active since peer was added, it returns the adding time.
	// If the connection is currently inactive, it returns zero time.
	// ActiveSince 返回该peer的活跃时间
	ActiveSince(id types.ID) time.Time
	// Stop closes the connections and stops the transporter.
	// Stop 关闭链接并停止该transporter
	Stop()
}

// Transport implements Transporter interface. It provides the functionality
// to send raft messages to peers, and receive raft messages from peers.
// User should call Handler method to get a handler to serve requests
// received from peerURLs.
// User needs to call Start before calling other functions, and call
// Stop when the Transport is no longer used.
// Transport 实现了Transporter接口。它提供功能，发送及接收raft信息到相应peers。
// 用户应该调用Handler放放来获取响应请求的handler
// 用户应该首先调用Start函数，并且在Transport不再使用时调用Stop
type Transport struct {
	DialTimeout time.Duration     // maximum duration before timing out dial of the request
	TLSInfo     transport.TLSInfo // TLS information used when creating connection

	ID          types.ID   // local member ID
	URLs        types.URLs // local peer URLs
	ClusterID   types.ID   // raft cluster ID for request validation
	Raft        Raft       // raft state machine, to which the Transport forwards received messages and reports status
	Snapshotter *snap.Snapshotter
	ServerStats *stats.ServerStats // used to record general transportation statistics
	// used to record transportation statistics with followers when
	// performing as leader in raft protocol
	LeaderStats *stats.LeaderStats
	// ErrorC is used to report detected critical errors, e.g.,
	// the member has been permanently removed from the cluster
	// When an error is received from ErrorC, user should stop raft state
	// machine and thus stop the Transport.
	ErrorC chan error

	streamRt   http.RoundTripper // roundTripper used by streams
	pipelineRt http.RoundTripper // roundTripper used by pipelines

	mu      sync.RWMutex         // protect the remote and peer map
	remotes map[types.ID]*remote // remotes map that helps newly joined member to catch up
	peers   map[types.ID]Peer    // peers map

	prober probing.Prober
}

// Start 启动Transport，做些基本的初始化复值
func (t *Transport) Start() error {
	var err error
	t.streamRt, err = newStreamRoundTripper(t.TLSInfo, t.DialTimeout)
	if err != nil {
		return err
	}
	t.pipelineRt, err = NewRoundTripper(t.TLSInfo, t.DialTimeout)
	if err != nil {
		return err
	}
	t.remotes = make(map[types.ID]*remote)
	t.peers = make(map[types.ID]Peer)
	t.prober = probing.NewProber(t.pipelineRt)
	return nil
}

// Handler
func (t *Transport) Handler() http.Handler {
	pipelineHandler := newPipelineHandler(t, t.Raft, t.ClusterID)
	streamHandler := newStreamHandler(t, t, t.Raft, t.ID, t.ClusterID)
	snapHandler := newSnapshotHandler(t, t.Raft, t.Snapshotter, t.ClusterID)
	mux := http.NewServeMux()
	mux.Handle(RaftPrefix, pipelineHandler)
	mux.Handle(RaftStreamPrefix+"/", streamHandler)
	mux.Handle(RaftSnapshotPrefix, snapHandler)
	mux.Handle(ProbingPrefix, probing.NewHandler())
	return mux
}

// Get
func (t *Transport) Get(id types.ID) Peer {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.peers[id]
}

// Send
func (t *Transport) Send(msgs []raftpb.Message) {
	for _, m := range msgs {
		if m.To == 0 {
			// ignore intentionally dropped message
			continue
		}
		to := types.ID(m.To)

		t.mu.RLock()
		p, pok := t.peers[to]
		g, rok := t.remotes[to]
		t.mu.RUnlock()

		if pok {
			if m.Type == raftpb.MsgApp {
				t.ServerStats.SendAppendReq(m.Size())
			}
			p.send(m)
			continue
		}

		if rok {
			g.send(m)
			continue
		}

		plog.Debugf("ignored message %s (sent to unknown peer %s)", m.Type, to)
	}
}

// Stop
func (t *Transport) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, r := range t.remotes {
		r.stop()
	}
	for _, p := range t.peers {
		p.stop()
	}
	t.prober.RemoveAll()
	if tr, ok := t.streamRt.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	if tr, ok := t.pipelineRt.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	t.peers = nil
	t.remotes = nil
}

// CutPeer drops messages to the specified peer.
// CutPeer 丢弃给指定peer的消息
func (t *Transport) CutPeer(id types.ID) {
	t.mu.RLock()
	p, pok := t.peers[id]
	g, gok := t.remotes[id]
	t.mu.RUnlock()

	if pok {
		p.(Pausable).Pause()
	}
	if gok {
		g.Pause()
	}
}

// MendPeer recovers the message dropping behavior of the given peer.
// MendPeer 恢复丢弃指定peer消息的行为
func (t *Transport) MendPeer(id types.ID) {
	t.mu.RLock()
	p, pok := t.peers[id]
	g, gok := t.remotes[id]
	t.mu.RUnlock()

	if pok {
		p.(Pausable).Resume()
	}
	if gok {
		g.Resume()
	}
}

// AddRemote
func (t *Transport) AddRemote(id types.ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.remotes == nil {
		// there's no clean way to shutdown the golang http server
		// (see: https://github.com/golang/go/issues/4674) before
		// stopping the transport; ignore any new connections.
		return
	}
	if _, ok := t.peers[id]; ok {
		return
	}
	if _, ok := t.remotes[id]; ok {
		return
	}
	urls, err := types.NewURLs(us)
	if err != nil {
		plog.Panicf("newURLs %+v should never fail: %+v", us, err)
	}
	t.remotes[id] = startRemote(t, urls, id)
}

// AddPeer
func (t *Transport) AddPeer(id types.ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.peers == nil {
		panic("transport stopped")
	}
	if _, ok := t.peers[id]; ok {
		return
	}
	urls, err := types.NewURLs(us)
	if err != nil {
		plog.Panicf("newURLs %+v should never fail: %+v", us, err)
	}
	fs := t.LeaderStats.Follower(id.String())
	t.peers[id] = startPeer(t, urls, id, fs)
	addPeerToProber(t.prober, id.String(), us)

	plog.Infof("added peer %s", id)
}

// RemovePeer
func (t *Transport) RemovePeer(id types.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.removePeer(id)
}

// RemoveAllPeers
func (t *Transport) RemoveAllPeers() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id := range t.peers {
		t.removePeer(id)
	}
}

// the caller of this function must have the peers mutex.
// removePeer
func (t *Transport) removePeer(id types.ID) {
	if peer, ok := t.peers[id]; ok {
		peer.stop()
	} else {
		plog.Panicf("unexpected removal of unknown peer '%d'", id)
	}
	delete(t.peers, id)
	delete(t.LeaderStats.Followers, id.String())
	t.prober.Remove(id.String())
	plog.Infof("removed peer %s", id)
}

// UpdatePeer
func (t *Transport) UpdatePeer(id types.ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// TODO: return error or just panic?
	if _, ok := t.peers[id]; !ok {
		return
	}
	urls, err := types.NewURLs(us)
	if err != nil {
		plog.Panicf("newURLs %+v should never fail: %+v", us, err)
	}
	t.peers[id].update(urls)

	t.prober.Remove(id.String())
	addPeerToProber(t.prober, id.String(), us)
	plog.Infof("updated peer %s", id)
}

// ActiveSince
func (t *Transport) ActiveSince(id types.ID) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	if p, ok := t.peers[id]; ok {
		return p.activeSince()
	}
	return time.Time{}
}

// SendSnapshot
func (t *Transport) SendSnapshot(m snap.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.peers[types.ID(m.To)]
	if p == nil {
		m.CloseWithError(errMemberNotFound)
		return
	}
	p.sendSnap(m)
}

// Pausable is a testing interface for pausing transport traffic.
// Pausable 是一个测试接口，来暂停transport
type Pausable interface {
	Pause()
	Resume()
}

func (t *Transport) Pause() {
	for _, p := range t.peers {
		p.(Pausable).Pause()
	}
}

func (t *Transport) Resume() {
	for _, p := range t.peers {
		p.(Pausable).Resume()
	}
}

type nopTransporter struct{}

func NewNopTransporter() Transporter {
	return &nopTransporter{}
}

func (s *nopTransporter) Start() error                        { return nil }
func (s *nopTransporter) Handler() http.Handler               { return nil }
func (s *nopTransporter) Send(m []raftpb.Message)             {}
func (s *nopTransporter) SendSnapshot(m snap.Message)         {}
func (s *nopTransporter) AddRemote(id types.ID, us []string)  {}
func (s *nopTransporter) AddPeer(id types.ID, us []string)    {}
func (s *nopTransporter) RemovePeer(id types.ID)              {}
func (s *nopTransporter) RemoveAllPeers()                     {}
func (s *nopTransporter) UpdatePeer(id types.ID, us []string) {}
func (s *nopTransporter) ActiveSince(id types.ID) time.Time   { return time.Time{} }
func (s *nopTransporter) Stop()                               {}
func (s *nopTransporter) Pause()                              {}
func (s *nopTransporter) Resume()                             {}

// snapTransporter
type snapTransporter struct {
	nopTransporter
	snapDoneC chan snap.Message
	snapDir   string
}

func NewSnapTransporter(snapDir string) (Transporter, <-chan snap.Message) {
	ch := make(chan snap.Message, 1)
	tr := &snapTransporter{snapDoneC: ch, snapDir: snapDir}
	return tr, ch
}

func (s *snapTransporter) SendSnapshot(m snap.Message) {
	ss := snap.New(s.snapDir)
	ss.SaveDBFrom(m.ReadCloser, m.Snapshot.Metadata.Index+1)
	m.CloseWithError(nil)
	s.snapDoneC <- m
}
