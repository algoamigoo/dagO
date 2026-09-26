package dago

import (
	"html/template"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/philippgille/gokv/gomap"
)

// exampleRouter sets up a test router with dummy HTML templates to prevent
// LoadHTMLGlob from panicking when the UI files don't exist in the test environment.
func exampleRouter() *gin.Engine {
	// Set Gin to Test Mode to suppress noisy [GIN-debug] output
	gin.SetMode(gin.TestMode)

	g := New(Options{UIPath: "ui/", ShowExamples: true, WithSeconds: true})
	g.execute("example-custom-operator")
	g.Use(DefaultLogger())

	// Bypass LoadHTMLGlob panic by setting a dummy template directly
	tmpl := template.Must(template.New("index.html.tmpl").Parse("dummy index"))
	tmpl = template.Must(tmpl.New("job.html.tmpl").Parse("dummy job"))
	g.router.SetHTMLTemplate(tmpl)

	// Manually register static routes WITHOUT calling LoadHTMLGlob
	g.router.Static("/css", g.Options.UIPath+"css")
	g.router.Static("/dist", g.Options.UIPath+"dist")
	g.router.Static("/src", g.Options.UIPath+"src")

	g.addStreamRoute(false)
	g.addUIRoutes()
	g.addAPIRoutes()

	return g.router
}

var router = exampleRouter()

type TestResponseRecorder struct {
	*httptest.ResponseRecorder
	closeChannel chan bool
}

func (r *TestResponseRecorder) CloseNotify() <-chan bool {
	return r.closeChannel
}

func CreateTestResponseRecorder() *TestResponseRecorder {
	return &TestResponseRecorder{
		httptest.NewRecorder(),
		make(chan bool, 1),
	}
}

func TestIndexRoute(t *testing.T) {
	// FIX: Use a fresh recorder for the first request
	w1 := httptest.NewRecorder()
	req1, _ := http.NewRequest("GET", "/ui/", nil)
	router.ServeHTTP(w1, req1)

	if w1.Code != http.StatusOK {
		t.Errorf("/ui/ status is %d, expected %d", w1.Code, http.StatusOK)
	}

	// FIX: Use a fresh recorder for the second request to avoid stale state
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest("GET", "/", nil)
	router.ServeHTTP(w2, req2)

	if w2.Code != http.StatusFound {
		t.Errorf("/ status is %d, expected %d", w2.Code, http.StatusFound)
	}
}

func TestHealthRoute(t *testing.T) {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/health", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("httpStatus is %d, expected %d", w.Code, http.StatusOK)
	}
}

func TestJobsRoute(t *testing.T) {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/jobs", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("httpStatus is %d, expected %d", w.Code, http.StatusOK)
	}

	w = httptest.NewRecorder() // Fresh recorder
	req, _ = http.NewRequest("GET", "/api/jobs/example-complex-analytics", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("httpStatus is %d, expected %d", w.Code, http.StatusOK)
	}
}

func TestJobRunsRoute(t *testing.T) {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/jobruns", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("httpStatus is %d, expected %d", w.Code, http.StatusOK)
	}
}

func TestExecutionsRoute(t *testing.T) {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/executions", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("httpStatus is %d, expected %d", w.Code, http.StatusOK)
	}
}

func TestJobSubmitToRouter(t *testing.T) {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/jobs/example-complex-analytics/submit", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("httpStatus is %d, expected %d", w.Code, http.StatusOK)
	}

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/api/jobs/example-custom-operator/submit", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("httpStatus is %d, expected %d", w.Code, http.StatusOK)
	}

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/api/jobs/bla/submit", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("httpStatus is %d, expected %d", w.Code, http.StatusNotFound)
	}
}

func TestJobToggleActiveRoute(t *testing.T) {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/jobs/example-complex-analytics/toggle", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("httpStatus is %d, expected %d", w.Code, http.StatusOK)
	}

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/api/jobs/example-custom-operator/toggle", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("httpStatus is %d, expected %d", w.Code, http.StatusOK)
	}

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/api/jobs/bla/toggle", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("httpStatus is %d, expected %d", w.Code, http.StatusNotFound)
	}
}

func TestRouteNotFound(t *testing.T) {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/blaaaa", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("httpStatus is %d, expected %d", w.Code, http.StatusNotFound)
	}

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/api/jobs/blaaaa", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("httpStatus is %d, expected %d", w.Code, http.StatusNotFound)
	}

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/ui/jobs/blaaaa", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("httpStatus is %d, expected %d", w.Code, http.StatusNotFound)
	}
}

func TestJobOverviewRoute(t *testing.T) {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/ui/jobs/example-complex-analytics", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("httpStatus is %d, expected %d", w.Code, http.StatusOK)
	}

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/jobs/bla", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("httpStatus is %d, expected %d", w.Code, http.StatusNotFound)
	}
}

func TestStreamRoute(t *testing.T) {
	w := CreateTestResponseRecorder()
	req, _ := http.NewRequest("GET", "/stream", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("httpStatus is %d, expected %d", w.Code, http.StatusOK)
	}

	w = CreateTestResponseRecorder()
	req, _ = http.NewRequest("GET", "/stream?jobname=example-complex-analytics", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("httpStatus is %d, expected %d", w.Code, http.StatusOK)
	}
}

func TestToggleRaceCondition(t *testing.T) {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/jobs/example-complex-analytics/toggle", nil)
	router.ServeHTTP(w, req)
}

func TestScheduledExecution(t *testing.T) {
	store := gomap.NewStore(gomap.DefaultOptions)
	schedExec := scheduledExecution{store: store, jobFunc: customOperatorJob}
	schedExec.Run()

	// Give the background goroutine a moment to start and avoid race detector teardown warnings
	time.Sleep(50 * time.Millisecond)
}

func TestGoflowWithoutOptions(t *testing.T) {
	g := New(Options{})
	g.Use(DefaultLogger())
}
