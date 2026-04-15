package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
	log "github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"
)

type (
	DBConn        = pgxpool.Conn
	Notifications = pgconn.Notification

	Listener struct {
		channel            string
		mu                 sync.Mutex
		onceStart          sync.Once
		onceStop           sync.Once
		wgListen           sync.WaitGroup // started via Go; Wait in Stop before UNLISTEN on the same conn
		notificationBuffer int            // capacity of notificationsCh; from NewListener / NewListenerFromPool; <1 treated as 1
		notificationsCh    chan *Notifications
		err                error
		conn               *DBConn
		cancelFunc         context.CancelFunc
	}
)

// NewListener creates a new Listener for channel on conn. notificationBuffer is the capacity of Notifications();
// values less than 1 are treated as 1.
func NewListener(conn *DBConn, channel string, notificationBuffer int) *Listener {
	return &Listener{
		channel:            channel,
		conn:               conn,
		notificationBuffer: notificationBuffer,
	}
}

// NewListenerFromPool acquires a connection from pool and returns a Listener for channel.
// notificationBuffer is the capacity of Notifications(); values less than 1 are treated as 1.
func NewListenerFromPool(ctx context.Context, pool *DBPool, channel string, notificationBuffer int) (*Listener, error) {
	if pool == nil {
		return nil, errors.New("pool is nil")
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	return NewListener(conn, channel, notificationBuffer), nil
}

func (l *Listener) setErr(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.err = err
}

func (l *Listener) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

// Notifications returns a channel to receive database notifications from the listener asynchronously.
func (l *Listener) Notifications() <-chan *Notifications {
	return l.notificationsCh
}

// Start initializes the listener to begin receiving notifications on the specified channel. Returns an error if failed.
func (l *Listener) Start(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is nil")
	}

	l.onceStart.Do(func() {
		var cancellable context.Context
		// nolint:gosec
		cancellable, l.cancelFunc = context.WithCancel(ctx)
		listenSQL := fmt.Sprintf("LISTEN %q;", l.channel)
		if cmd, err := l.conn.Exec(ctx, listenSQL); err != nil {
			log.Error(err, log.F("cmd", cmd.String()))
			l.setErr(err)
			return
		}

		buf := max(l.notificationBuffer, 1)
		l.notificationsCh = make(chan *pgconn.Notification, buf)
		listenCtx := cancellable
		l.wgListen.Go(func() {
			defer close(l.notificationsCh)
			for {
				n, err := l.conn.Conn().WaitForNotification(listenCtx)
				if err != nil {
					l.setErr(err)
					return
				}

				l.notificationsCh <- n
			}
		})
	})
	return l.Err()
}

// Stop gracefully stops the listener by canceling the context and releasing the database connection.
func (l *Listener) Stop(timeout time.Duration) {
	l.onceStop.Do(func() {
		if l.cancelFunc != nil {
			l.cancelFunc()
		}
		l.wgListen.Wait()
		if l.conn != nil {
			timeoutCtx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			_, err := l.conn.Exec(timeoutCtx, fmt.Sprintf("UNLISTEN %q;", l.channel))
			if err != nil {
				log.Error(err)
			}

			l.conn.Release()
		}
	})
}

// MapNotificationToLogFields maps the notification to a slice of logger fields.
func MapNotificationToLogFields(n *Notifications) []logger.Field {
	return []logger.Field{
		log.F("channel", n.Channel),
		log.F("payload", n.Payload),
	}
}
