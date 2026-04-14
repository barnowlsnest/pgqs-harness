package postgres

import (
	"time"

	"github.com/barnowlsnest/go-asynctasklib/v2/pkg/task"

	log "github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"
)

func logFailedAttempt(_ uint64, _ time.Time, err error) {
	log.Error(err)
}

func logSuccessAttempt(id uint64, when time.Time) {
	log.Info("postgres ping attempt is successful")
}

func taskHooks() *task.StateHooks {
	return task.NewStateHooks(
		task.WhenFailed(logFailedAttempt),
		task.WhenDone(logSuccessAttempt),
	)
}
