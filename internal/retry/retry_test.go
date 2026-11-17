package retry

import (
	"context"
	"errors"
	"net/textproto"
	"syscall"
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
		kind Kind
	}{
		{"nil", nil, KindFatal},
		{"restart", ErrRestart, KindRestart},
		{"cancel", context.Canceled, KindFatal},
		{"deadline", context.DeadlineExceeded, KindTransient},
		{"421", &textproto.Error{Code: 421, Msg: "too many"}, KindLimit},
		{"426", &textproto.Error{Code: 426, Msg: "closed"}, KindTransient},
		{"550", &textproto.Error{Code: 550, Msg: "missing"}, KindFatal},
		{"530", &textproto.Error{Code: 530, Msg: "login"}, KindFatal},
		{"reset", syscall.ECONNRESET, KindTransient},
		{"auth", errors.New("ssh: unable to authenticate"), KindFatal},
		{"stall", errors.New("read stall timeout"), KindTransient},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.err); got != tc.kind {
				t.Fatalf("Classify(%v) = %v, want %v", tc.err, got, tc.kind)
			}
		})
	}
}

func TestBackoffBounds(t *testing.T) {
	for attempt := 1; attempt <= 8; attempt++ {
		delay := Backoff(attempt, time.Second, 4*time.Second)
		if delay < 500*time.Millisecond || delay > 4*time.Second {
			t.Fatalf("attempt %d delay %s out of range", attempt, delay)
		}
	}
}

func TestSleepCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Sleep(ctx, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep err = %v", err)
	}
}
