package dago

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func (g *Goflow) addStaticRoutes() *Goflow {
	g.router.Static("/css", g.Options.UIPath+"css")
	g.router.Static("/dist", g.Options.UIPath+"dist")
	g.router.Static("/src", g.Options.UIPath+"src")
	g.router.LoadHTMLGlob(g.Options.UIPath + "html/*.html.tmpl")
	return g
}

func (g *Goflow) addStreamRoute(keepOpen bool) *Goflow {
	g.router.GET("/stream", g.stream(keepOpen))
	return g
}

type jobrun struct {
	JobName   string   `json:"job"`
	Submitted string   `json:"submitted"`
	JobState  jobstate `json:"state"`
}

type jobstate struct {
	State     state     `json:"job"`
	TaskState taskstate `json:"tasks"`
}

type taskstate struct {
	Taskstate map[string]state `json:"state"`
}

func (g *Goflow) addAPIRoutes() *Goflow {
	api := g.router.Group("/api")
	{
		api.GET("/health", func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{"health": "OK"})
		})

		api.GET("/jobs", func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{"jobs": g.jobs})
		})

		// Deprecated: will be removed in v3.0.0
		api.GET("/jobruns", func(c *gin.Context) {
			jobName := c.Query("jobname")
			stateQuery := c.Query("state")

			jobruns := make([]jobrun, 0)

			for job := range g.Jobs {
				stored, _ := readExecutions(g.Store, job)
				for _, execution := range stored {
					// FIXED: Clean filtering logic using continue
					if stateQuery != "" && stateQuery != string(execution.State) {
						continue
					}
					if jobName != "" && jobName != execution.JobName {
						continue
					}

					t := taskstate{Taskstate: make(map[string]state)}

					for _, task := range execution.TaskExecutions {
						t.Taskstate[task.Name] = task.State
					}

					j := jobrun{
						JobName:   execution.JobName, // Use execution's job name for accuracy
						Submitted: execution.StartedAt,
						JobState: jobstate{
							State:     execution.State,
							TaskState: t,
						},
					}

					jobruns = append(jobruns, j)
				}
			}

			c.JSON(http.StatusOK, gin.H{"jobruns": jobruns})
		})

		api.GET("/executions", func(c *gin.Context) {
			jobName := c.Query("jobname")
			stateQuery := c.Query("state")

			executions := make([]*execution, 0)

			for job := range g.Jobs {
				stored, _ := readExecutions(g.Store, job)
				for _, execution := range stored {
					// FIXED: Clean filtering logic using continue
					if stateQuery != "" && stateQuery != string(execution.State) {
						continue
					}
					if jobName != "" && jobName != execution.JobName {
						continue
					}
					executions = append(executions, execution)
				}
			}

			c.JSON(http.StatusOK, gin.H{"executions": executions})
		})

		api.GET("/jobs/:name", func(c *gin.Context) {
			name := c.Param("name")
			jobFn, ok := g.Jobs[name]

			var msg struct {
				JobName   string   `json:"job"`
				TaskNames []string `json:"tasks"`
				Dag       dag      `json:"dag"`
				Schedule  string   `json:"schedule"`
				Active    bool     `json:"active"`
			}

			if ok {
				j := jobFn() // FIXED: Call factory exactly once to avoid redundant allocations
				msg.JobName = name
				msg.TaskNames = j.tasks
				msg.Dag = j.Dag
				msg.Schedule = j.Schedule

				// check if the job is active by looking in the list of cron entries
				for _, entry := range g.cron.Entries() {
					if entryName := entry.Job.(*scheduledExecution).jobFunc().Name; entryName == name {
						msg.Active = true
						break // FIXED: Added break for optimization
					}
				}

				c.JSON(http.StatusOK, msg)
			} else {
				c.JSON(http.StatusNotFound, msg)
			}
		})

		api.POST("/jobs/:name/submit", func(c *gin.Context) {
			name := c.Param("name")
			_, ok := g.Jobs[name]

			var msg struct {
				Job         string `json:"job"`
				Success     bool   `json:"success"`
				Submitted   string `json:"submitted"`
				ExecutionID string `json:"execution_id,omitempty"` // FIXED: Added to response for client tracking
			}
			msg.Job = name

			if ok {
				execID := g.execute(name) // FIXED: Capture the returned UUID
				msg.Success = true
				msg.Submitted = time.Now().UTC().Format(time.RFC3339Nano)
				msg.ExecutionID = execID.String()
				c.JSON(http.StatusOK, msg)
			} else {
				msg.Success = false
				c.JSON(http.StatusNotFound, msg)
			}
		})

		api.POST("/jobs/:name/toggle", func(c *gin.Context) {
			name := c.Param("name")
			_, ok := g.Jobs[name]

			var msg struct {
				Job     string `json:"job"`
				Success bool   `json:"success"`
				Active  bool   `json:"active"`
				Error   string `json:"error,omitempty"` // FIXED: Added error field
			}
			msg.Job = name

			if ok {
				isActive, err := g.toggle(name) // FIXED: Handle the error properly
				if err != nil {
					msg.Success = false
					msg.Error = err.Error()
					c.JSON(http.StatusInternalServerError, msg)
					return
				}
				msg.Success = true
				msg.Active = isActive
				c.JSON(http.StatusOK, msg)
			} else {
				msg.Success = false
				c.JSON(http.StatusNotFound, msg)
			}
		})
	}

	return g
}

func (g *Goflow) addUIRoutes() *Goflow {
	ui := g.router.Group("/ui")
	{
		ui.GET("/", func(c *gin.Context) {
			jobs := make([]*Job, 0)
			for _, job := range g.jobs {

				// create the job, assume it's inactive
				j := g.Jobs[job]()
				j.Active = false

				// check if the job is active by looking in the list of cron entries
				for _, entry := range g.cron.Entries() {
					if name := entry.Job.(*scheduledExecution).jobFunc().Name; name == j.Name {
						j.Active = true
					}
				}

				jobs = append(jobs, j)
			}
			c.HTML(http.StatusOK, "index.html.tmpl", gin.H{"jobs": jobs})
		})

		ui.GET("/jobs/:name", func(c *gin.Context) {
			name := c.Param("name")
			jobFn, ok := g.Jobs[name]

			if ok {
				j := jobFn() // FIXED: Call factory exactly once
				c.HTML(http.StatusOK, "job.html.tmpl", gin.H{
					"jobName":   name,
					"taskNames": j.tasks,
					"schedule":  j.Schedule,
				})
			} else {
				c.String(http.StatusNotFound, "Not found")
			}
		})
	}

	g.router.GET("/", func(c *gin.Context) {
		c.Redirect(http.StatusFound, "/ui/")
	})

	return g
}
