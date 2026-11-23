package webui

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateDownloadIsIdempotentAndValidatesPaths(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "incoming", "tile.tif")
	svc := &fakeService{}
	handler := handlerFor(t, svc, false)
	body := `{
		"external_id":" batch-42 ",
		"source":"ftp://delivery@ftp.example:21/incoming/tile.tif",
		"destination":"` + dest + `",
		"password":"s3cret",
		"priority":4,
		"metadata":{"sensor":"landsat"}
	}`
	first := postDownload(t, handler, body)
	if first.Code != http.StatusCreated || strings.Contains(first.Body.String(), "s3cret") || !strings.Contains(first.Body.String(), `"external_id": "batch-42"`) || !strings.Contains(first.Body.String(), `"priority": 4`) || !strings.Contains(first.Body.String(), "landsat") {
		t.Fatalf("create %d %s", first.Code, first.Body.String())
	}
	if !strings.Contains(first.Body.String(), `"remote": "/incoming/tile.tif"`) || !strings.Contains(first.Body.String(), `"host": "ftp.example"`) {
		t.Fatalf("source was not applied: %s", first.Body.String())
	}
	second := postDownload(t, handler, body)
	if second.Code != http.StatusConflict || !strings.Contains(second.Body.String(), `"code": "external_id_conflict"`) || !strings.Contains(second.Body.String(), `"id": "created"`) || strings.Contains(second.Body.String(), "s3cret") {
		t.Fatalf("conflict %d %s", second.Code, second.Body.String())
	}
	relative := postDownload(t, handler, `{"source":"/incoming/a","destination":"relative.bin","host":"ftp.example","user":"delivery","password":"s3cret","external_id":"other"}`)
	if relative.Code != http.StatusBadRequest {
		t.Fatalf("relative %d %s", relative.Code, relative.Body.String())
	}
	withPassword := postDownload(t, handler, `{"source":"ftp://delivery:s3cret@ftp.example/incoming/a","destination":"`+dest+`","external_id":"pw"}`)
	if withPassword.Code != http.StatusBadRequest || strings.Contains(withPassword.Body.String(), "s3cret") {
		t.Fatalf("url password %d %s", withPassword.Code, withPassword.Body.String())
	}
}

func postDownload(t *testing.T, handler tokenHandler, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/api/v1/jobs", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+handler.token)
	request.Header.Set("Content-Type", "application/json")
	request.Host = "127.0.0.1"
	recorded := httptest.NewRecorder()
	handler.ServeHTTP(recorded, request)
	return recorded
}
