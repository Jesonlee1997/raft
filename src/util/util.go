package util

import (
	crand "crypto/rand"
	"encoding/base64"
	"encoding/json"
	"time"
)

// timeout工具类
func Timeout(ch chan interface{}, timeout time.Duration) (val interface{}, isTimeout bool) {
	timeCh := make(chan bool, 1)
	go func() {
		time.Sleep(timeout)
		timeCh <- true
	}()

	select {
	case val := <-ch:
		return val, false
	case <-timeCh:
		// election timeout,转变状态，开始一轮选举
		close(timeCh)
		isTimeout = true
		return
	}
}

func GetMillDuration(i int64) time.Duration {
	return time.Duration(i * int64(time.Millisecond))
}

func CloseChan(ch chan interface{}) {
	close(ch)
	for _, open := <-ch; open; {
		_, open = <-ch
	}
}

func Min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func String(v interface{}) string {
	bytes, _ := json.Marshal(v)
	return string(bytes)
}

func Randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}