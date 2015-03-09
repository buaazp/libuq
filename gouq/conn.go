package uq

import "time"

type Conn interface {
	add(topic string, line string, recycle time.Duration) error
	push(key string, value []byte) error
	pop(key string) (string, []byte, error)
	del(key string) error
	close()
}
