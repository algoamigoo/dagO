package dago

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/philippgille/gokv"
)

// state defines the lifecycle stage of a task or job.
type state string

const (
	none       state = "notstarted"
	running    state = "running"
	upForRetry state = "upforretry"
	skipped    state = "skipped"
	failed     state = "failed"
	successful state = "successful"
)

// writeOp is used internally to pass task state changes via a channel.
type writeOp struct {
	key string
	val state
}

// execution represents a single run of a job.
type execution struct {
	ID                string
	State             state
	ModifiedTimestamp string
}

// Job is a workflow consisting of independent and dependent tasks.
type Job struct {
	Name     string
	Tasks    map[string]*Task
	Schedule string
	Dag      dag
	Active   bool
	state    state
	tasks    []string
	sync.RWMutex
}

// Initialize a job.
func (j *Job) initialize() *Job {
	j.Dag = make(dag)
	j.Tasks = make(map[string]*Task)
	j.tasks = make([]string, 0)
	j.storeState(none)
	return j
}

// Add a task to a job.
func (j *Job) Add(t *Task) *Job {
	if j.Dag == nil {
		j.initialize()
	}

	if t.TriggerRule != allDone && t.TriggerRule != allSuccessful {
		t.TriggerRule = allSuccessful
	}

	t.remaining = t.Retries

	j.Lock()
	j.Tasks[t.Name] = t
	j.tasks = append(j.tasks, t.Name)
	j.Dag.addNode(t.Name)
	j.Unlock()

	j.storeTaskState(t.Name, none)
	return j
}

// Task getter
func (j *Job) Task(name string) *Task {
	j.RLock()
	defer j.RUnlock()
	return j.Tasks[name]
}

// SetDownstream sets a dependency relationship between two tasks.
func (j *Job) SetDownstream(ind, dep *Task) *Job {
	j.Lock()
	j.Dag.setDownstream(ind.Name, dep.Name)
	j.Unlock()
	return j
}

func (j *Job) loadState() state {
	j.RLock()
	defer j.RUnlock()

	if j.allSuccessful() {
		return successful
	}
	if j.allDone() && j.anyFailed() {
		return failed
	}
	if !j.allDone() {
		return running
	}
	return j.state
}

func (j *Job) loadTaskState(task string) state {
	j.RLock()
	defer j.RUnlock()
	if t, ok := j.Tasks[task]; ok {
		return t.state
	}
	return none
}

func (j *Job) storeState(value state) {
	j.Lock()
	j.state = value
	j.Unlock()
}

func (j *Job) storeTaskState(task string, value state) {
	j.Lock()
	if t, ok := j.Tasks[task]; ok {
		t.state = value
	}
	j.Unlock()
}

// run executes the job workflow using a gokv.Store for persistence.
func (j *Job) run(ctx context.Context, store gokv.Store, e *execution) error {
	if !j.Dag.validate() {
		return fmt.Errorf("invalid DAG for job %s", j.Name)
	}

	log.Printf("jobID=%v, jobname=%v, msg=starting", e.ID, j.Name)

	// Buffered channel to prevent goroutines from blocking if the loop is busy
	writes := make(chan writeOp, len(j.Tasks))

	// 1. Start the initial independent tasks before entering the loop
	j.Lock()
	for _, task := range j.Tasks {
		if !j.Dag.isDownstream(task.Name) && task.state == none {
			task.state = running
			log.Printf("jobID=%v, job=%v, task=%v, msg=starting", e.ID, j.Name, task.Name)
			go task.run(ctx, writes)
		}
	}
	j.Unlock()

	// 2. Event Loop: React to task completions or context cancellation
	for {
		select {
		case <-ctx.Done():
			log.Printf("jobID=%v, job=%v, msg=context_cancelled", e.ID, j.Name)
			return ctx.Err()

		case write := <-writes:
			// Update in-memory state
			j.storeTaskState(write.key, write.val)
			log.Printf("jobID=%v, job=%v, task=%v, msg=%v", e.ID, j.Name, write.key, write.val)

			// Sync to gokv store
			e.State = j.loadState()
			e.ModifiedTimestamp = time.Now().UTC().Format(time.RFC3339Nano)

			syncStateToStore(store, e, write.key, write.val)

			// 3. Evaluate downstream tasks to see if they can start now
			j.evaluateDownstreamTasks(ctx, e.ID, writes)

			// 4. Check if the entire job is finished
			if j.allDone() {
				finalState := j.loadState()
				log.Printf("jobID=%v, job=%v, msg=%v", e.ID, j.Name, finalState)
				return nil // Returning here removes the "unreachable code" error
			}
		}
	}
}

// evaluateDownstreamTasks checks pending downstream tasks and starts them
// if their trigger rules have been met.
func (j *Job) evaluateDownstreamTasks(ctx context.Context, jobID string, writes chan<- writeOp) {
	j.Lock()
	defer j.Unlock()

	for _, task := range j.Tasks {
		if task.state != none {
			continue
		}
		if !j.Dag.isDownstream(task.Name) {
			continue
		}

		if task.state == upForRetry {
			task.state = running
			go task.run(ctx, writes)
			continue
		}

		upstreamDone := true
		upstreamSuccessful := true

		for _, us := range j.Dag.dependencies(task.Name) {
			usState := j.Tasks[us].state
			if usState == none || usState == running || usState == upForRetry {
				upstreamDone = false
			}
			if usState != successful {
				upstreamSuccessful = false
			}
		}

		if upstreamDone && task.TriggerRule == allDone {
			task.state = running
			log.Printf("jobID=%v, job=%v, task=%v, msg=starting", jobID, j.Name, task.Name)
			go task.run(ctx, writes)
		} else if upstreamSuccessful && task.TriggerRule == allSuccessful {
			task.state = running
			log.Printf("jobID=%v, job=%v, task=%v, msg=starting", jobID, j.Name, task.Name)
			go task.run(ctx, writes)
		} else if upstreamDone && !upstreamSuccessful && task.TriggerRule == allSuccessful {
			task.state = skipped
			log.Printf("jobID=%v, job=%v, task=%v, msg=skipping", jobID, j.Name, task.Name)
			go task.skip(writes)
		}
	}
}

func (j *Job) allDone() bool {
	j.RLock()
	defer j.RUnlock()
	for _, t := range j.Tasks {
		if t.state == none || t.state == running || t.state == upForRetry {
			return false
		}
	}
	return true
}

func (j *Job) allSuccessful() bool {
	j.RLock()
	defer j.RUnlock()
	for _, t := range j.Tasks {
		if t.state != successful {
			return false
		}
	}
	return true
}

func (j *Job) anyFailed() bool {
	j.RLock()
	defer j.RUnlock()
	for _, t := range j.Tasks {
		if t.state == failed {
			return true
		}
	}
	return false
}

// syncStateToStore persists the execution and task state to the gokv store.
func syncStateToStore(store gokv.Store, e *execution, taskName string, taskState state) {
	if store == nil {
		return
	}

	// Save overall execution state
	execKey := fmt.Sprintf("execution:%s", e.ID)
	execBytes, _ := json.Marshal(e)
	_ = store.Set(execKey, execBytes)

	// Save individual task state
	taskKey := fmt.Sprintf("execution:%s:task:%s", e.ID, taskName)
	_ = store.Set(taskKey, string(taskState))
}
