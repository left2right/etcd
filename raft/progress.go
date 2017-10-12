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

package raft

import "fmt"

const (
	ProgressStateProbe ProgressStateType = iota
	ProgressStateReplicate
	ProgressStateSnapshot
)

type ProgressStateType uint64

var prstmap = [...]string{
	"ProgressStateProbe",
	"ProgressStateReplicate",
	"ProgressStateSnapshot",
}

func (st ProgressStateType) String() string { return prstmap[uint64(st)] }

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
// Progress 表示以leader视角看follower的进度。Leader维护所有follower的进度，并且根据每个follower
// 的进去发送entries
type Progress struct {
	Match, Next uint64
	// State defines how the leader should interact with the follower.
	//
	// When in ProgressStateProbe, leader sends at most one replication message
	// per heartbeat interval. It also probes actual progress of the follower.
	//
	// When in ProgressStateReplicate, leader optimistically increases next
	// to the latest entry sent after sending replication message. This is
	// an optimized state for fast replicating log entries to the follower.
	//
	// When in ProgressStateSnapshot, leader should have sent out snapshot
	// before and stops sending any replication message.
	// State 状态定义leader应该如何跟follower交互
	// 当在ProgressStateProbe状态时，leader每个心跳最多发送一条复制信息。探测follower
	// 真实的进度。
	// 当在ProgressStateReplicate状态，leader在发送完复制信息后乐观的增加next到最新的entry
	// 这是一个为了快速复制log entries到follower的一个乐观优化状态。
	// 当在ProgressStateSnapshot状态时，leader应该停止发送任何复制信息，并且发送snapshot
	State ProgressStateType
	// Paused is used in ProgressStateProbe.
	// When Paused is true, raft should pause sending replication message to this peer.
	// Paused 在ProgressStateProbe状态时使用，当Paused为true时，raft应该暂停发送复制信息到这个peer。
	Paused bool
	// PendingSnapshot is used in ProgressStateSnapshot.
	// If there is a pending snapshot, the pendingSnapshot will be set to the
	// index of the snapshot. If pendingSnapshot is set, the replication process of
	// this Progress will be paused. raft will not resend snapshot until the pending one
	// is reported to be failed.
	// PendingSnapshot 在ProgressStateSnapshot状态时使用。如果有一个pending的snapshot，pendingSnapshot
	// 就设置为这个snapshot的index。如果pendingSnapshot设置了，这个节点的Progress将会暂停。raft将不再发送
	// snapshot直到这个pending 的报错
	PendingSnapshot uint64

	// RecentActive is true if the progress is recently active. Receiving any messages
	// from the corresponding follower indicates the progress is active.
	// RecentActive can be reset to false after an election timeout.
	// RecentActive 是ture的状态，如果这个progress最近是活跃的。从相应的follower收到任何消息说明这个progress
	// 是活跃的。RecentActive在一个选举超时后被设置为false
	RecentActive bool

	// inflights is a sliding window for the inflight messages.
	// Each inflight message contains one or more log entries.
	// The max number of entries per message is defined in raft config as MaxSizePerMsg.
	// Thus inflight effectively limits both the number of inflight messages
	// and the bandwidth each Progress can use.
	// When inflights is full, no more message should be sent.
	// When a leader sends out a message, the index of the last
	// entry should be added to inflights. The index MUST be added
	// into inflights in order.
	// When a leader receives a reply, the previous inflights should
	// be freed by calling inflights.freeTo with the index of the last
	// received entry.
	// inflights 是inflight信息的滑动窗口，每个inflight信息包含一个或者多个entries。
	ins *inflights
}

// resetState 重置Progress的状态
func (pr *Progress) resetState(state ProgressStateType) {
	pr.Paused = false
	pr.PendingSnapshot = 0
	pr.State = state
	pr.ins.reset()
}

// becomeProbe 将Progress状态设置为ProgressStateProbe，如果之前状态是ProgressStateSnapshot
// 则将Next设置为pendingSnapshot+1，否则Next设置为pr.Match + 1
func (pr *Progress) becomeProbe() {
	// If the original state is ProgressStateSnapshot, progress knows that
	// the pending snapshot has been sent to this peer successfully, then
	// probes from pendingSnapshot + 1.
	if pr.State == ProgressStateSnapshot {
		pendingSnapshot := pr.PendingSnapshot
		pr.resetState(ProgressStateProbe)
		pr.Next = max(pr.Match+1, pendingSnapshot+1)
	} else {
		pr.resetState(ProgressStateProbe)
		pr.Next = pr.Match + 1
	}
}

// becomeReplicate 将Progress状态设置为ProgressStateReplicate, Next设置为pr.Match + 1
func (pr *Progress) becomeReplicate() {
	pr.resetState(ProgressStateReplicate)
	pr.Next = pr.Match + 1
}

// becomeSnapshot 将Progress状态设置为ProgressStateSnapshot,PendingSnapshot复值为snapshoti
func (pr *Progress) becomeSnapshot(snapshoti uint64) {
	pr.resetState(ProgressStateSnapshot)
	pr.PendingSnapshot = snapshoti
}

// maybeUpdate returns false if the given n index comes from an outdated message.
// Otherwise it updates the progress and returns true.
// maybeUpdate 更新progress的index，如果index n是过期的，则更新失败。如果Match小于n，则更新Match
// 到n，并且将Paused状态设置为false，更新成功。如果Next小于n+1，将Next更新为n+1
func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
		pr.resume()
	}
	if pr.Next < n+1 {
		pr.Next = n + 1
	}
	return updated
}

// optimisticUpdate 不做任何检查的将Next更新为n+1
func (pr *Progress) optimisticUpdate(n uint64) { pr.Next = n + 1 }

// maybeDecrTo returns false if the given to index comes from an out of order message.
// Otherwise it decreases the progress next index to min(rejected, last) and returns true.
// maybeDecrTo 根据rejected和last index调整降低Next index到相应位置。如果rejected不是一个新鲜的
// 则函数返回false。
func (pr *Progress) maybeDecrTo(rejected, last uint64) bool {
	if pr.State == ProgressStateReplicate {
		// the rejection must be stale if the progress has matched and "rejected"
		// is smaller than "match".
		if rejected <= pr.Match {
			return false
		}
		// directly decrease next to match + 1
		pr.Next = pr.Match + 1
		return true
	}

	// the rejection must be stale if "rejected" does not match next - 1
	if pr.Next-1 != rejected {
		return false
	}

	if pr.Next = min(rejected, last+1); pr.Next < 1 {
		pr.Next = 1
	}
	pr.resume()
	return true
}

func (pr *Progress) pause()  { pr.Paused = true }
func (pr *Progress) resume() { pr.Paused = false }

// IsPaused returns whether sending log entries to this node has been
// paused. A node may be paused because it has rejected recent
// MsgApps, is currently waiting for a snapshot, or has reached the
// MaxInflightMsgs limit.
// IsPaused 检查发送log entries到这个节点是否已经暂停了。一个节点是暂停状态可能是
// 拒绝了最近的MsgApps，或者等待一个snapshot，或者达到了MaxInflightMsgs限制
func (pr *Progress) IsPaused() bool {
	switch pr.State {
	case ProgressStateProbe:
		return pr.Paused
	case ProgressStateReplicate:
		return pr.ins.full()
	case ProgressStateSnapshot:
		return true
	default:
		panic("unexpected state")
	}
}

func (pr *Progress) snapshotFailure() { pr.PendingSnapshot = 0 }

// needSnapshotAbort returns true if snapshot progress's Match
// is equal or higher than the pendingSnapshot.
// needSnapshotAbort 返回true，如果progress状态是ProgressStateSnapshot，并且Match大于等于PendingSnapshot
func (pr *Progress) needSnapshotAbort() bool {
	return pr.State == ProgressStateSnapshot && pr.Match >= pr.PendingSnapshot
}

func (pr *Progress) String() string {
	return fmt.Sprintf("next = %d, match = %d, state = %s, waiting = %v, pendingSnapshot = %d", pr.Next, pr.Match, pr.State, pr.IsPaused(), pr.PendingSnapshot)
}

// inflights 结构体
type inflights struct {
	// the starting index in the buffer
	start int
	// number of inflights in the buffer
	count int

	// the size of the buffer
	size int

	// buffer contains the index of the last entry
	// inside one message.
	buffer []uint64
}

// newInflights 创建返回inflights
func newInflights(size int) *inflights {
	return &inflights{
		size: size,
	}
}

// add adds an inflight into inflights
// add 添加inflight到inflights，更新相应的size，count和buffer
func (in *inflights) add(inflight uint64) {
	if in.full() {
		panic("cannot add into a full inflights")
	}
	next := in.start + in.count
	size := in.size
	//循环
	if next >= size {
		next -= size
	}
	if next >= len(in.buffer) {
		in.growBuf()
	}
	in.buffer[next] = inflight
	in.count++
}

// grow the inflight buffer by doubling up to inflights.size. We grow on demand
// instead of preallocating to inflights.size to handle systems which have
// thousands of Raft groups per process.
// growBuf 通过两倍inflights的size增大inflight的buffer。我们根据需求增大buffer，而不是预先分配
// 到inflights.size，来应对每个进程有上千的raft groups
func (in *inflights) growBuf() {
	newSize := len(in.buffer) * 2
	if newSize == 0 {
		newSize = 1
	} else if newSize > in.size {
		newSize = in.size
	}
	newBuffer := make([]uint64, newSize)
	copy(newBuffer, in.buffer)
	in.buffer = newBuffer
}

// freeTo frees the inflights smaller or equal to the given `to` flight.
// freeTo 释放inflights到to，不比这个大的全部释放掉
func (in *inflights) freeTo(to uint64) {
	if in.count == 0 || to < in.buffer[in.start] {
		// out of the left side of the window
		return
	}

	i, idx := 0, in.start
	for i = 0; i < in.count; i++ {
		if to < in.buffer[idx] { // found the first large inflight
			break
		}

		// increase index and maybe rotate
		size := in.size
		if idx++; idx >= size {
			idx -= size
		}
	}
	// free i inflights and set new start index
	in.count -= i
	in.start = idx
	if in.count == 0 {
		// inflights is empty, reset the start index so that we don't grow the
		// buffer unnecessarily.
		in.start = 0
	}
}

func (in *inflights) freeFirstOne() { in.freeTo(in.buffer[in.start]) }

// full returns true if the inflights is full.
func (in *inflights) full() bool {
	return in.count == in.size
}

// resets frees all inflights.
func (in *inflights) reset() {
	in.count = 0
	in.start = 0
}
