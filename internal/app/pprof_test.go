package dsync

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMuxesDoNotExposeCommandLine(t *testing.T) {
	for name, mux := range map[string]*http.ServeMux{
		"progress": progressMux(func(http.ResponseWriter, *http.Request) {}),
		"pprof":    pprofMux(),
	} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/cmdline", nil))
		assert.NotContains(t, rec.Body.String(), strings.Join(os.Args, "\x00"), name)
		assert.Equal(t, http.StatusNotFound, rec.Code, name)
	}
}
