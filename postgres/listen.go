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
		channel         string
		mu              sync.Mutex
		onceStart       sync.Once
		onceStop        sync.Once
		notificationsCh chan *Notifications
		err             error
		conn            *DBConn
		cancelFunc      context.CancelFunc
	}
)

// NewListener creates and returns a new Listener instance for the specified database connection and channel.
func NewListener(conn *DBConn, channel string) *Listener {
	return &Listener{
		channel: channel,
		conn:    conn,
	}
}

// NewListenerFromPool creates a Listener instance using a connection from the provided pool and the specified channel.
func NewListenerFromPool(ctx context.Context, pool *DBPool, channel string) (*Listener, error) {
	if pool == nil {
		return nil, errors.New("pool is nil")
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	return NewListener(conn, channel), nil
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
		cancellable, l.cancelFunc = context.WithCancel(ctx)
		listenSQL := fmt.Sprintf("LISTEN %q;", l.channel)
		if cmd, err := l.conn.Exec(ctx, listenSQL); err != nil {
			log.Error(err, log.F("cmd", cmd.String()))
			l.setErr(err)
			return
		}

		l.notificationsCh = make(chan *pgconn.Notification, 1)
		go func(ctx context.Context, l *Listener) {
			defer close(l.notificationsCh)
			for {
				n, err := l.conn.Conn().WaitForNotification(ctx)
				if err != nil {
					l.setErr(err)
					return
				}

				l.notificationsCh <- n
			}
		}(cancellable, l)
	})
	return l.Err()
}

// Stop gracefully stops the listener by canceling the context and releasing the database connection.
func (l *Listener) Stop(timeout time.Duration) {
	l.onceStop.Do(func() {
		if l.cancelFunc != nil {
			l.cancelFunc()
		}
		if l.conn != nil {
			timeoutCtx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			_, err := l.conn.Exec(timeoutCtx, fmt.Sprintf("UNLISTEN %s", l.channel))
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
