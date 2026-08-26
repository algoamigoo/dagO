# DAGO

A lightweight, extensible **DAG-based workflow scheduler and execution engine written in Go**.

DAGFlow allows you to define workflows as directed acyclic graphs, schedule them using cron expressions, execute independent tasks concurrently, retry failed tasks, and monitor executions in real time through a web dashboard.

## Features

* **DAG-based workflows** — Define complex task dependencies as directed acyclic graphs.
* **Concurrent execution** — Independent tasks are executed concurrently using Go's concurrency primitives.
* **Task dependencies** — Tasks execute only when their upstream dependencies satisfy their trigger conditions.
* **Retries** — Configurable retry attempts with constant-delay and exponential-backoff strategies.
* **Trigger rules** — Control whether tasks execute based on the state of their upstream dependencies.
* **Cron scheduling** — Schedule workflows using cron expressions.
* **Custom operators** — Extend the scheduler with custom task implementations.
* **Persistent execution history** — Store workflow and task execution state in PostgreSQL or Redis.
* **REST API** — Submit jobs, inspect workflows, and query execution history.
* **Real-time monitoring** — Stream task and workflow status to the dashboard using Server-Sent Events.
* **Web dashboard** — Monitor active and completed workflow executions.

---

## Architecture

```text
                         ┌──────────────────┐
                         │    REST API      │
                         │                  │
                         │ Submit / Query   │
                         └────────┬─────────┘
                                  │
                                  ▼
                       ┌────────────────────┐
                       │   DAG Scheduler    │
                       │                    │
                       │ Dependency Resolver│
                       │ Cron Scheduler     │
                       │ Execution Manager  │
                       └─────────┬──────────┘
                                 │
                    ┌────────────┼────────────┐
                    │            │            │
                    ▼            ▼            ▼
                Task A        Task B        Task C
                              (parallel)
                    │            │            │
                    └────────────┼────────────┘
                                 ▼
                              Task D
                                 │
                                 ▼
                       ┌────────────────────┐
                       │      Storage       │
                       │ PostgreSQL / Redis │
                       └────────────────────┘

                                 │
                                 ▼
                       ┌────────────────────┐
                       │   SSE / Dashboard │
                       └────────────────────┘
```

---

## Core Concepts

### Job

A **Job** represents a workflow.

A job contains:

* A unique name
* A collection of tasks
* A DAG describing task dependencies
* An optional cron schedule
* Execution configuration

Example:

```go
job := NewJob("payment-processing")

job.AddTask(validate)
job.AddTask(authorize)
job.AddTask(capture)

job.AddDependency(validate, authorize)
job.AddDependency(authorize, capture)
```

This produces:

```text
Validate
   │
   ▼
Authorize
   │
   ▼
Capture
```

---

### Task

A **Task** represents an individual unit of work within a job.

A task contains:

```text
Task
 ├── Name
 ├── Operator
 ├── Dependencies
 ├── Retry Policy
 ├── Trigger Rule
 └── Execution State
```

Example:

```go
Task{
    Name:     "authorize-payment",
    Operator: AuthorizePayment{},
    Retries:  3,
}
```

---

## DAG Execution

DAGFlow resolves dependencies before executing a task.

Consider:

```text
          Validate
          /      \
         ▼        ▼
    Authorize   FraudCheck
         \        /
          ▼      ▼
          Capture
```

`Authorize` and `FraudCheck` have the same dependency and can therefore execute concurrently.

```text
Time ──────────────────────────────►

Validate    █████

Authorize         █████████
FraudCheck        ███████

Capture                     ███████
```

This avoids unnecessarily serializing independent work.

---

## Task States

Each task moves through a defined lifecycle:

```text
             ┌─────────┐
             │ PENDING │
             └────┬────┘
                  │
                  ▼
              ┌───────┐
              │ READY │
              └───┬───┘
                  │
                  ▼
             ┌─────────┐
             │ RUNNING │
             └────┬────┘
                /   \
               ▼     ▼
        ┌─────────┐ ┌────────┐
        │ SUCCESS │ │ FAILED │
        └─────────┘ └────┬───┘
                          │
                          ▼
                     ┌──────────┐
                     │ RETRYING │
                     └────┬─────┘
                          │
                          ▼
                        READY
```

A task becomes `READY` when its dependency conditions are satisfied.

---

## Trigger Rules

By default, a task executes only when all of its upstream dependencies succeed.

Example:

```text
       A
      / \
     B   C
      \ /
       D
```

`D` executes only when:

```text
B = SUCCESS
C = SUCCESS
```

Additional trigger rules can be used for workflows that need cleanup or recovery operations.

For example:

```text
        Main Task
            │
            ▼
       Cleanup Task
```

The cleanup task can be configured to execute regardless of whether the upstream task succeeded or failed.

---

## Retries

Tasks can define a retry policy.

Example:

```go
RetryPolicy{
    MaxAttempts: 5,
    Backoff:     ExponentialBackoff{
        InitialDelay: time.Second,
        Multiplier:   2,
    },
}
```

A failed task can therefore execute:

```text
Attempt 1
   │
   └── failure
          │
          ▼
       1 second
          │
          ▼
Attempt 2
   │
   └── failure
          │
          ▼
       2 seconds
          │
          ▼
Attempt 3
```

This prevents transient failures from immediately causing an entire workflow to fail.

---

## Cron Scheduling

Jobs can optionally be scheduled using cron expressions.

Example:

```go
Job{
    Name:     "daily-settlement",
    Schedule: "0 2 * * *",
}
```

The scheduler automatically creates a new execution according to the configured schedule.

---

## Operators

An operator defines the actual work performed by a task.

DAGFlow provides a simple interface for implementing custom operators:

```go
type Operator interface {
    Run(ctx context.Context) (any, error)
}
```

Example:

```go
type AddNumbers struct {
    A int
    B int
}

func (o AddNumbers) Run(ctx context.Context) (any, error) {
    return o.A + o.B, nil
}
```

This makes the execution engine independent from the actual business logic performed by tasks.

---

## REST API

The scheduler exposes APIs for interacting with workflows.

### Health

```http
GET /api/health
```

### List Jobs

```http
GET /api/jobs
```

### Get Job

```http
GET /api/jobs/:name
```

### Submit Job

```http
POST /api/jobs/:name/submit
```

### List Executions

```http
GET /api/executions
```

### Get Execution

```http
GET /api/executions/:id
```

---

## Real-Time Execution Streaming

DAGFlow uses **Server-Sent Events (SSE)** to stream execution updates to the dashboard.

Example:

```text
event: task_update

data:
{
  "job": "payment-processing",
  "task": "authorize",
  "status": "RUNNING"
}
```

The dashboard can therefore display execution progress without continuously polling the API.

---

## Storage

Execution history is persisted through a storage abstraction.

The storage layer is designed so the execution engine is not tightly coupled to a specific database.

Possible implementations:

```text
             Storage Interface
                    │
          ┌─────────┼─────────┐
          ▼         ▼         ▼
      PostgreSQL   Redis    In-Memory
```

Stored information includes:

* Job definitions
* Execution IDs
* Task states
* Start/end timestamps
* Error information
* Retry attempts
* Execution results

---

## Concurrency Model

DAGFlow uses Go's concurrency primitives to execute independent tasks concurrently.

Conceptually:

```go
go execute(taskA)
go execute(taskB)
go execute(taskC)
```

Synchronization is used to ensure that downstream tasks are not scheduled until their dependency conditions are satisfied.

The scheduler is responsible for:

1. Identifying runnable tasks.
2. Starting task execution.
3. Tracking task state.
4. Detecting completion.
5. Evaluating downstream dependencies.
6. Scheduling newly available tasks.

---

## Example Workflow

A payment workflow could be represented as:

```text
                 Validate Payment
                  /           \
                 ▼             ▼
          Fraud Detection   Check Balance
                 \             /
                  ▼           ▼
                    Authorize
                       │
                       ▼
                    Capture
                       │
                       ▼
                  Send Receipt
```

DAGFlow can execute:

```text
Validate
   │
   ├──────► Fraud Detection ────┐
   │                            │
   └──────► Check Balance ──────┤
                                ▼
                            Authorize
                                │
                                ▼
                             Capture
                                │
                                ▼
                          Send Receipt
```

The independent fraud and balance checks run concurrently.

---

## Tech Stack

| Component         | Technology                    |
| ----------------- | ----------------------------- |
| Language          | Go                            |
| API               | REST                          |
| Scheduling        | Cron                          |
| Database          | PostgreSQL / Redis            |
| Real-time Updates | Server-Sent Events            |
| Concurrency       | Goroutines, Channels, Mutexes |
| Frontend          | React / JavaScript            |
| Containerization  | Docker                        |

---

## Running Locally

### Prerequisites

* Go 1.XX+
* PostgreSQL or Redis
* Node.js
* Docker *(optional)*

### Clone

```bash
git clone <your-repository-url>
cd dagflow
```

### Install Go dependencies

```bash
go mod download
```

### Start the server

```bash
go run ./cmd/server
```

### Start the dashboard

```bash
cd ui
npm install
npm run dev
```

The API and dashboard will then be available locally.

---

## Project Structure

```text
dago/
├── cmd/
│   ├── server/
│   └── worker/
│
├── engine/
│   ├── scheduler.go
│   ├── executor.go
│   └── dependency.go
│
├── dag/
│   ├── dag.go
│   └── task.go
│
├── operators/
│   ├── operator.go
│   ├── command.go
│   └── http.go
│
├── storage/
│   ├── store.go
│   ├── postgres.go
│   └── redis.go
│
├── api/
│   ├── routes.go
│   └── handlers.go
│
├── stream/
│   └── sse.go
│
├── ui/
│
├── examples/
│
├── go.mod
└── README.md
```

---

## Roadmap

* [x] DAG representation
* [x] Dependency resolution
* [x] Concurrent task execution
* [x] Custom operators
* [ ] Retry strategies
* [ ] Trigger rules
* [ ] Cron scheduling
* [ ] Persistent storage
* [ ] REST API
* [ ] SSE execution streaming
* [ ] Web dashboard
* [ ] Metrics and observability
* [ ] Docker deployment
* [ ] Comprehensive unit/integration tests

---

## Goals

The goal of DAGFlow is to explore the engineering challenges behind workflow orchestration systems while keeping the implementation small enough to understand and extend.

The project focuses on:

* DAG scheduling
* Concurrent execution
* Dependency resolution
* Failure handling
* Retry strategies
* Persistent workflow state
* API design
* Real-time execution monitoring
* Go concurrency
