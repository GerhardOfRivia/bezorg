package webui

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/GerhardOfRivia/bezorg/internal/config"
	"github.com/GerhardOfRivia/bezorg/internal/control"
	"github.com/GerhardOfRivia/bezorg/internal/job"
)

type apiServer struct {
	svc        control.Service
	publicRead bool
}

func (api apiServer) routes() map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		"GET /api/v1/system":            api.system,
		"GET /api/v1/jobs":              api.list,
		"GET /api/v1/events":            api.events,
		"GET /api/v1/jobs/{id}":         api.get,
		"GET /api/v1/jobs/{id}/logs":    api.logs,
		"POST /api/v1/jobs/{id}/cancel": api.cancel,
		"POST /api/v1/jobs/{id}/retry":  api.retry,
	}
}

type systemResponse struct {
	control.System
	PublicRead bool `json:"public_read"`
}

func (api apiServer) system(output http.ResponseWriter, request *http.Request) {
	system, err := api.svc.System(request.Context())
	if err != nil {
		writeServiceError(output, err)
		return
	}
	writeJSON(output, http.StatusOK, systemResponse{System: system, PublicRead: api.publicRead})
}

func (api apiServer) list(output http.ResponseWriter, request *http.Request) {
	externalID := strings.TrimSpace(request.URL.Query().Get("external_id"))
	if externalID != "" {
		if _, err := config.NormalizeExternalID(externalID); err != nil {
			writeAPIError(output, http.StatusBadRequest, err.Error())
			return
		}
	}
	jobs, err := api.svc.List(request.Context(), externalID)
	if err != nil {
		writeServiceError(output, err)
		return
	}
	if jobs == nil {
		jobs = []job.Record{}
	}
	for i := range jobs {
		jobs[i] = job.Public(jobs[i])
	}
	writeJSON(output, http.StatusOK, map[string]any{"jobs": jobs})
}

func (api apiServer) get(output http.ResponseWriter, request *http.Request) {
	rec, err := api.svc.Get(request.Context(), request.PathValue("id"))
	if err != nil {
		writeServiceError(output, err)
		return
	}
	writeJSON(output, http.StatusOK, map[string]any{"job": job.Public(rec)})
}

func (api apiServer) logs(output http.ResponseWriter, request *http.Request) {
	after, _ := strconv.Atoi(request.URL.Query().Get("after"))
	lines, next, err := api.svc.Logs(request.Context(), request.PathValue("id"), after)
	if err != nil {
		writeServiceError(output, err)
		return
	}
	if lines == nil {
		lines = []string{}
	}
	writeJSON(output, http.StatusOK, map[string]any{"lines": lines, "next": next})
}

func (api apiServer) cancel(output http.ResponseWriter, request *http.Request) {
	rec, err := api.svc.Cancel(request.Context(), request.PathValue("id"))
	if err != nil {
		writeServiceError(output, err)
		return
	}
	writeJSON(output, http.StatusOK, map[string]any{"job": job.Public(rec)})
}

func (api apiServer) retry(output http.ResponseWriter, request *http.Request) {
	rec, err := api.svc.Retry(request.Context(), request.PathValue("id"))
	if err != nil {
		writeServiceError(output, err)
		return
	}
	writeJSON(output, http.StatusOK, map[string]any{"job": job.Public(rec)})
}

func writeServiceError(output http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, job.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, job.ErrAmbiguous):
		status = http.StatusConflict
	default:
		if strings.Contains(err.Error(), "cannot be retried") {
			status = http.StatusConflict
		}
	}
	writeAPIError(output, status, err.Error())
}

func writeAPIError(output http.ResponseWriter, status int, message string) {
	writeJSON(output, status, map[string]string{"error": message})
}

func writeJSON(output http.ResponseWriter, status int, body any) {
	output.Header().Set("Content-Type", "application/json")
	output.Header().Set("Cache-Control", "no-store")
	output.WriteHeader(status)
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(body)
}
