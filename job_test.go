package dago

import (
	"context"
	"testing"
	"time"

	"github.com/philippgille/gokv/gomap"
)

func TestJob(t *testing.T) {
	j := &Job{Name: "example", Schedule: "* * * * *"}

	j.Add(&Task{
		Name:     "add-one-one",
		Operator: Command{Cmd: "sh", Args: []string{"-c", "echo $((1 + 1))"}},
	})
	j.Add(&Task{
		Name:     "sleep-two",
		Operator: Command{Cmd: "sleep", Args: []string{"2"}},
	})
	j.Add(&Task{
		Name:     "add-two-four",
		Operator: Command{Cmd: "sh", Args: []string{"-c", "echo $((2 + 4))"}},
	})
	j.Add(&Task{
		Name:     "add-three-four",
		Operator: Command{Cmd: "sh", Args: []string{"-c", "echo $((3 + 4))"}},
	})
	j.Add(&Task{
		Name:       "whoops-with-constant-delay",
		Operator:   Command{Cmd: "whoops", Args: []string{}},
		Retries:    5,
		RetryDelay: ConstantDelay{Period: 1},
	})
	j.Add(&Task{
		Name:       "whoops-with-exponential-backoff",
		Operator:   Command{Cmd: "whoops", Args: []string{}},
		Retries:    1,
		RetryDelay: ExponentialBackoff{},
	})
	j.Add(&Task{
		Name:        "totally-skippable",
		Operator:    Command{Cmd: "sh", Args: []string{"-c", "echo 'everything succeeded'"}},
		TriggerRule: allSuccessful,
	})
	j.Add(&Task{
		Name:        "clean-up",
		Operator:    Command{Cmd: "sh", Args: []string{"-c", "echo 'cleaning up now'"}},
		TriggerRule: allDone,
	})
	j.Add(&Task{
		Name:     "failure",
		Operator: RandomFailure{n: 1},
	})

	j.SetDownstream(j.Task("add-one-one"), j.Task("sleep-two"))
	j.SetDownstream(j.Task("sleep-two"), j.Task("add-two-four"))
	j.SetDownstream(j.Task("add-one-one"), j.Task("add-three-four"))
	j.SetDownstream(j.Task("add-one-one"), j.Task("whoops-with-constant-delay"))
	j.SetDownstream(j.Task("add-one-one"), j.Task("whoops-with-exponential-backoff"))
	j.SetDownstream(j.Task("whoops-with-constant-delay"), j.Task("totally-skippable"))
	j.SetDownstream(j.Task("whoops-with-exponential-backoff"), j.Task("totally-skippable"))
	j.SetDownstream(j.Task("totally-skippable"), j.Task("clean-up"))

	store := gomap.NewStore(gomap.DefaultOptions)

	go j.run(context.Background(), store, j.newExecution())

	for !j.allDone() {
		time.Sleep(10 * time.Millisecond)
	}

	if j.Tasks["add-one-one"].state != successful {
		t.Errorf("Got status %v, expected %v", j.Tasks["add-one-one"].state, successful)
	}
	if j.Tasks["sleep-two"].state != successful {
		t.Errorf("Got status %v, expected %v", j.Tasks["sleep-two"].state, successful)
	}
	if j.Tasks["add-two-four"].state != successful {
		t.Errorf("Got status %v, expected %v", j.Tasks["add-two-four"].state, successful)
	}
	if j.Tasks["add-three-four"].state != successful {
		t.Errorf("Got status %v, expected %v", j.Tasks["add-three-four"].state, successful)
	}
	if j.Tasks["whoops-with-constant-delay"].state != failed {
		t.Errorf("Got status %v, expected %v", j.Tasks["whoops-with-constant-delay"].state, failed)
	}
	if j.Tasks["whoops-with-exponential-backoff"].state != failed {
		t.Errorf("Got status %v, expected %v", j.Tasks["whoops-with-exponential-backoff"].state, failed)
	}
	if j.Tasks["totally-skippable"].state != skipped {
		t.Errorf("Got status %v, expected %v", j.Tasks["totally-skippable"].state, skipped)
	}
	if j.Tasks["clean-up"].state != successful {
		t.Errorf("Got status %v, expected %v", j.Tasks["clean-up"].state, successful)
	}
	if j.Tasks["failure"].state != failed {
		t.Errorf("Got status %v, expected %v", j.Tasks["failure"].state, failed)
	}
}

func TestCyclicJob(t *testing.T) {
	j := &Job{Name: "cyclic", Schedule: "* * * * *"}

	j.Add(&Task{
		Name:     "add-two-four",
		Operator: Command{Cmd: "sh", Args: []string{"-c", "echo $((2 + 4))"}},
	})
	j.Add(&Task{
		Name:     "add-three-four",
		Operator: Command{Cmd: "sh", Args: []string{"-c", "echo $((3 + 4))"}},
	})

	j.SetDownstream(j.Task("add-two-four"), j.Task("add-three-four"))
	j.SetDownstream(j.Task("add-three-four"), j.Task("add-two-four"))

	store := gomap.NewStore(gomap.DefaultOptions)

	// FIX 4: Added context.Background() and assert that the cyclic DAG correctly returns an error
	err := j.run(context.Background(), store, j.newExecution())
	if err == nil {
		t.Errorf("Expected an error for a cyclic DAG, but got nil")
	}
}
