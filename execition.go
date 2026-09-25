package dago

import (
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/philippgille/gokv"
)

// execution represents a single run of a job.
type execution struct {
	ID                uuid.UUID       `json:"id"`
	JobName           string          `json:"job"`
	StartedAt         string          `json:"submitted"`
	ModifiedTimestamp string          `json:"modifiedTimestamp"`
	State             state           `json:"state"`
	TaskExecutions    []taskExecution `json:"tasks"`
}

// taskExecution is the per-task state captured inside an execution.
type taskExecution struct {
	Name  string `json:"name"`
	State state  `json:"state"`
}

// newExecution builds an execution snapshot from the job's current task list.
func (j *Job) newExecution() *execution {
	j.RLock()
	defer j.RUnlock()

	tasks := make([]taskExecution, 0, len(j.Tasks))
	for _, t := range j.Tasks {
		tasks = append(tasks, taskExecution{Name: t.Name, State: none})
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return &execution{
		ID:                uuid.New(),
		JobName:           j.Name,
		StartedAt:         now,
		ModifiedTimestamp: now,
		State:             none,
		TaskExecutions:    tasks,
	}
}

// updateTaskState mutates the in-memory execution snapshot.
func (e *execution) updateTaskState(name string, s state) {
	for i := range e.TaskExecutions {
		if e.TaskExecutions[i].Name == name {
			e.TaskExecutions[i].State = s
			return
		}
	}
}

// persistNewExecution stores the execution under its UUID key.
func persistNewExecution(s gokv.Store, e *execution) error {
	if s == nil {
		return nil
	}
	return s.Set(e.ID.String(), e)
}

// executionIndex is a per-job list of execution IDs, used for listing/history.
type executionIndex struct {
	ExecutionIDs []string `json:"executions"`
}

// indexExecutions appends the execution ID to the per-job index.
func indexExecutions(s gokv.Store, e *execution) error {
	if s == nil {
		return nil
	}
	idx := executionIndex{}

	// gokv's Get returns (found bool, err error)
	if _, err := s.Get(e.JobName, &idx); err != nil {
		return err
	}
	idx.ExecutionIDs = append(idx.ExecutionIDs, e.ID.String())
	return s.Set(e.JobName, idx)
}

// readExecutions returns all persisted executions for a job name.
func readExecutions(s gokv.Store, jobName string) ([]*execution, error) {
	idx := executionIndex{}

	// gokv's Get returns (found bool, err error)
	found, err := s.Get(jobName, &idx)
	if err != nil {
		return nil, err
	}

	out := make([]*execution, 0)
	if !found {
		return out, nil
	}

	for _, id := range idx.ExecutionIDs {
		var e execution
		found, err := s.Get(id, &e)
		if err != nil {
			log.Printf("readExecutions: skipping %s: %v", id, err)
			continue
		}
		if !found {
			continue
		}
		out = append(out, &e)
	}
	return out, nil
}

// syncStateToStore updates the in-memory task state and persists the whole execution.
func syncStateToStore(s gokv.Store, e *execution, taskName string, taskState state) error {
	if s == nil {
		return nil
	}
	e.updateTaskState(taskName, taskState)
	e.ModifiedTimestamp = time.Now().UTC().Format(time.RFC3339Nano)
	return s.Set(e.ID.String(), e)
}
