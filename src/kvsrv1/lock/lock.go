package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	key      string
	clientID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:       ck,
		key:      l,
		clientID: kvtest.RandValue(8),
	}
	// You may add code here

	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	// 确保对应的值是空的，才可以修改值，如果网络原因导致不确定修改是否成功，再查询一次
	for {
		if val, ver, err := lk.ck.Get(lk.key); err == rpc.ErrNoKey || val == "" { // 锁释放
			if err := lk.ck.Put(lk.key, lk.clientID, ver); err == rpc.OK {
				// 锁获取成功
				return
			} else if err == rpc.ErrMaybe { // 锁可能获取成功, 需要验证
				if val, _, err := lk.ck.Get(lk.key); err == rpc.OK && val == lk.clientID {
					// 锁获取成功
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	// 确保对应的值是自己的客户端id，才可以修改值为空
	if val, ver, err := lk.ck.Get(lk.key); err == rpc.OK && val == lk.clientID {
		// 键已经存在，已确保其他客户端没有put的情况下，返回值只有OK和ErrMaybe
		// ErrMaybe， 版本对应不上导致的，由于只有当前客户端先将值置空才能成功改变版本，所以返回ErrMaybe，锁必定是释放成功的
		lk.ck.Put(lk.key, "", ver)
	}
}
