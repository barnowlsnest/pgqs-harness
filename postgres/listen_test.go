//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/barnowlsnest/pgqs-harness/postgres"
)

// nilListenCtx returns a nil [context.Context] for exercising Start's nil guard without passing
// the nil literal to Start (staticcheck SA1012 only matches explicit nil in the call).
func nilListenCtx() context.Context {
	return nil
}

type ListenerSuite struct {
	suite.Suite
	ctx       context.Context
	container *tcpostgres.PostgresContainer
	pool      *pgxpool.Pool
}

func TestListenerSuite(t *testing.T) {
	suite.Run(t, new(ListenerSuite))
}

func (s *ListenerSuite) SetupSuite() {
	if testing.Short() {
		s.T().Skip("docker-backed listener tests (omit -short to run)")
	}

	s.ctx = context.Background()

	ctr, err := tcpostgres.Run(s.ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("listener_test"),
		tcpostgres.WithUsername("listener"),
		tcpostgres.WithPassword("listener"),
		tcpostgres.BasicWaitStrategies(),
	)
	s.Require().NoError(err)
	s.container = ctr

	connStr, err := ctr.ConnectionString(s.ctx, "sslmode=disable")
	s.Require().NoError(err)

	pool, err := pgxpool.New(s.ctx, connStr)
	s.Require().NoError(err)
	s.pool = pool

	s.Require().NoError(pool.Ping(s.ctx))
}

func (s *ListenerSuite) TearDownSuite() {
	if s.pool != nil {
		s.pool.Close()
	}
	if s.container != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s.Require().NoError(s.container.Terminate(shutdownCtx))
	}
}

func (s *ListenerSuite) TestMapNotificationToLogFields() {
	n := &pgconn.Notification{Channel: "events", Payload: "payload-data"}
	fields := postgres.MapNotificationToLogFields(n)
	s.Require().Len(fields, 2)

	byKey := make(map[string]interface{}, len(fields))
	for _, f := range fields {
		byKey[f.Key] = f.Value
	}
	s.Equal("events", byKey["channel"])
	s.Equal("payload-data", byKey["payload"])
}

func (s *ListenerSuite) TestNewListenerFromPool_nilPool() {
	l, err := postgres.NewListenerFromPool(s.ctx, nil, "ch", 1)
	s.Nil(l)
	s.Require().Error(err)
}

func (s *ListenerSuite) TestStart_nilContext() {
	conn, err := s.pool.Acquire(s.ctx)
	s.Require().NoError(err)

	listener := postgres.NewListener(conn, "test_ch", 1)
	s.Require().Error(listener.Start(nilListenCtx()))
	listener.Stop(time.Second)
}

func (s *ListenerSuite) TestNotify_roundTrip() {
	listener, err := postgres.NewListenerFromPool(s.ctx, s.pool, "test_ch", 1)
	s.Require().NoError(err)
	defer listener.Stop(10 * time.Second)

	s.Require().NoError(listener.Start(s.ctx))

	publisher, err := s.pool.Acquire(s.ctx)
	s.Require().NoError(err)
	defer publisher.Release()

	_, err = publisher.Exec(s.ctx, "NOTIFY test_ch, 'hello'")
	s.Require().NoError(err)

	select {
	case n := <-listener.Notifications():
		s.Require().NotNil(n)
		s.Equal("test_ch", n.Channel)
		s.Equal("hello", n.Payload)
	case <-time.After(15 * time.Second):
		s.Fail("timed out waiting for notification")
	}

	s.NoError(listener.Err())
}

func (s *ListenerSuite) TestNotify_fourSequentialInOrder() {
	const channel = "burst_ch"

	listener, err := postgres.NewListenerFromPool(s.ctx, s.pool, channel, 1)
	s.Require().NoError(err)
	defer listener.Stop(10 * time.Second)

	s.Require().NoError(listener.Start(s.ctx))

	publisher, err := s.pool.Acquire(s.ctx)
	s.Require().NoError(err)
	defer publisher.Release()

	for i := range 4 {
		want := strconv.Itoa(i)
		_, err = publisher.Exec(s.ctx, fmt.Sprintf("NOTIFY burst_ch, '%s'", want))
		s.Require().NoError(err)

		select {
		case n := <-listener.Notifications():
			s.Require().NotNil(n)
			s.Equal(channel, n.Channel, "message index %d", i)
			s.Equal(want, n.Payload, "message index %d", i)
		case <-time.After(15 * time.Second):
			s.Fail(fmt.Sprintf("timed out waiting for notification %d of 4", i))
		}
	}

	s.NoError(listener.Err())
}

func (s *ListenerSuite) TestNotify_defaultNotificationBufferOne() {
	listener, err := postgres.NewListenerFromPool(s.ctx, s.pool, "cap_default_ch", 0)
	s.Require().NoError(err)
	defer listener.Stop(10 * time.Second)

	s.Require().NoError(listener.Start(s.ctx))
	s.Equal(1, cap(listener.Notifications()))
}

func (s *ListenerSuite) TestNotify_fourBufferedBurstBeforeReceive() {
	const channel = "buffer_burst_ch"

	listener, err := postgres.NewListenerFromPool(s.ctx, s.pool, channel, 4)
	s.Require().NoError(err)
	defer listener.Stop(10 * time.Second)

	s.Require().NoError(listener.Start(s.ctx))
	s.Equal(4, cap(listener.Notifications()))

	publisher, err := s.pool.Acquire(s.ctx)
	s.Require().NoError(err)
	defer publisher.Release()

	for i := range 4 {
		want := strconv.Itoa(i)
		_, err := publisher.Exec(s.ctx, fmt.Sprintf("NOTIFY buffer_burst_ch, '%s'", want))
		s.Require().NoError(err)
	}

	ch := listener.Notifications()
	for i := range 4 {
		want := strconv.Itoa(i)
		select {
		case n := <-ch:
			s.Require().NotNil(n)
			s.Equal(channel, n.Channel)
			s.Equal(want, n.Payload)
		case <-time.After(15 * time.Second):
			s.Fail(fmt.Sprintf("timed out draining buffered notification %d of 4", i))
		}
	}

	s.NoError(listener.Err())
}

func (s *ListenerSuite) TestNotify_quotedChannel_casePreserved() {
	const channel = "MyNotifyChan"

	listener, err := postgres.NewListenerFromPool(s.ctx, s.pool, channel, 1)
	s.Require().NoError(err)
	defer listener.Stop(10 * time.Second)

	s.Require().NoError(listener.Start(s.ctx))

	publisher, err := s.pool.Acquire(s.ctx)
	s.Require().NoError(err)
	defer publisher.Release()

	_, err = publisher.Exec(s.ctx, `NOTIFY "MyNotifyChan", 'quoted-case'`)
	s.Require().NoError(err)

	select {
	case n := <-listener.Notifications():
		s.Require().NotNil(n)
		s.Equal(channel, n.Channel)
		s.Equal("quoted-case", n.Payload)
	case <-time.After(15 * time.Second):
		s.Fail("timed out waiting for notification on quoted channel")
	}

	s.NoError(listener.Err())
}
