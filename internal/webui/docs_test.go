package webui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpenAPICoversRoutesAndDocsArePublic(t *testing.T) {
	var document struct {
		Paths map[string]map[string]any `json:"paths"`
	}
	if err := json.Unmarshal(openAPISpec, &document); err != nil {
		t.Fatal(err)
	}
	api := apiServer{}
	for pattern := range api.routes() {
		method, path, ok := strings.Cut(pattern, " ")
		if !ok {
			t.Fatalf("bad pattern %s", pattern)
		}
		item := document.Paths[path]
		if item == nil || item[strings.ToLower(method)] == nil {
			t.Fatalf("OpenAPI is missing %s", pattern)
		}
	}
	operations := 0
	for path, item := range document.Paths {
		for method := range item {
			pattern := strings.ToUpper(method) + " " + path
			if _, ok := api.routes()[pattern]; !ok {
				t.Fatalf("OpenAPI operation %s is not a route", pattern)
			}
			operations++
		}
	}
	if operations != len(api.routes()) {
		t.Fatalf("openapi operations = %d, routes = %d", operations, len(api.routes()))
	}

	handler := handlerFor(t, &fakeService{}, false)
	spec := docsGet(t, handler, "/openapi.json")
	if spec.Code != http.StatusOK || !strings.Contains(spec.Body.String(), `"openapi": "3.1.0"`) {
		t.Fatalf("spec %d %s", spec.Code, spec.Body.String())
	}
	page := docsGet(t, handler, "/docs/")
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "X-bezorg-Signature") || !strings.Contains(page.Body.String(), "external_id") || !strings.Contains(page.Body.String(), "download.succeeded") {
		t.Fatalf("docs %d %s", page.Code, page.Body.String())
	}
	css := docsGet(t, handler, "/docs/docs.css")
	if css.Code != http.StatusOK || !strings.Contains(css.Header().Get("Content-Type"), "text/css") {
		t.Fatalf("css %d %s", css.Code, css.Header().Get("Content-Type"))
	}
	redirect := docsGet(t, handler, "/docs")
	if redirect.Code != http.StatusPermanentRedirect || redirect.Header().Get("Location") != "/docs/" {
		t.Fatalf("redirect %d location %s", redirect.Code, redirect.Header().Get("Location"))
	}
}

func docsGet(t *testing.T, handler tokenHandler, path string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1"+path, nil)
	request.Host = "127.0.0.1"
	recorded := httptest.NewRecorder()
	handler.ServeHTTP(recorded, request)
	return recorded
}
