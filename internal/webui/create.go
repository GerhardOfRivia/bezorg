package webui

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/GerhardOfRivia/bezorg/internal/config"
	"github.com/GerhardOfRivia/bezorg/internal/job"
	"github.com/GerhardOfRivia/bezorg/internal/paths"
)

const maxDownloadBody = 64 << 10

type createDownloadRequest struct {
	config.Config
	Source      string          `json:"source"`
	Destination string          `json:"destination"`
	Priority    *int            `json:"priority"`
	Metadata    json.RawMessage `json:"metadata"`
}

type downloadConflictResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
	Job job.Record `json:"job"`
}

func (api apiServer) createDownload(output http.ResponseWriter, request *http.Request) {
	var body createDownloadRequest
	if err := decodeDownload(request, &body); err != nil {
		writeAPIError(output, http.StatusBadRequest, err.Error())
		return
	}
	cfg := body.Config
	if err := applySource(&cfg, body.Source); err != nil {
		writeAPIError(output, http.StatusBadRequest, err.Error())
		return
	}
	if dest := strings.TrimSpace(body.Destination); dest != "" {
		if cfg.Local != "" && cfg.Local != dest {
			writeAPIError(output, http.StatusBadRequest, "destination and local disagree")
			return
		}
		cfg.Local = dest
	}
	if strings.TrimSpace(cfg.Local) == "" || !strings.HasPrefix(cfg.Local, "/") {
		writeAPIError(output, http.StatusBadRequest, "destination must be an absolute path")
		return
	}
	if _, err := paths.SafeLocal(cfg.Local, ""); err != nil {
		writeAPIError(output, http.StatusBadRequest, err.Error())
		return
	}
	externalID, err := config.NormalizeExternalID(cfg.ExternalID)
	if err != nil {
		writeAPIError(output, http.StatusBadRequest, err.Error())
		return
	}
	cfg.ExternalID = externalID
	priority := 0
	if body.Priority != nil {
		priority = *body.Priority
	}
	rec, err := api.svc.SubmitDownload(request.Context(), cfg, priority, body.Metadata)
	if errors.Is(err, job.ErrExternalIDInUse) {
		conflict := downloadConflictResponse{Job: rec}
		conflict.Error.Code = "external_id_conflict"
		conflict.Error.Message = "A download with this external_id already exists"
		writeJSON(output, http.StatusConflict, conflict)
		return
	}
	if err != nil {
		message := err.Error()
		if strings.HasPrefix(message, "config:") || strings.HasPrefix(message, "destination") || strings.HasPrefix(message, "job:") || strings.HasPrefix(message, "source") {
			writeAPIError(output, http.StatusBadRequest, message)
			return
		}
		writeAPIError(output, http.StatusServiceUnavailable, message)
		return
	}
	writeJSON(output, http.StatusCreated, map[string]any{"job": rec})
}

func applySource(cfg *config.Config, source string) error {
	source = strings.TrimSpace(source)
	if source == "" {
		return nil
	}
	if strings.ContainsRune(source, 0) {
		return errors.New("source cannot contain NUL")
	}
	if !strings.Contains(source, "://") {
		if cfg.Remote != "" && cfg.Remote != source {
			return errors.New("source and remote disagree")
		}
		cfg.Remote = source
		return nil
	}
	parsed, err := url.Parse(source)
	if err != nil || parsed.Host == "" || parsed.Path == "" {
		return errors.New("source must be a remote path or an ftp, ftps, or sftp URL")
	}
	switch parsed.Scheme {
	case config.ProtocolFTP, config.ProtocolFTPS, config.ProtocolSFTP:
		cfg.Protocol = parsed.Scheme
	default:
		return errors.New("source URL scheme must be ftp, ftps, or sftp")
	}
	if parsed.User != nil {
		if _, ok := parsed.User.Password(); ok {
			return errors.New("source URL must not contain a password; set password separately")
		}
		if user := parsed.User.Username(); user != "" {
			cfg.User = user
		}
	}
	cfg.Host = parsed.Hostname()
	if rawPort := parsed.Port(); rawPort != "" {
		port, err := strconv.Atoi(rawPort)
		if err != nil {
			return errors.New("source URL port is invalid")
		}
		cfg.Port = port
	}
	if cfg.Remote != "" && cfg.Remote != parsed.Path {
		return errors.New("source and remote disagree")
	}
	cfg.Remote = parsed.Path
	if parsed.Fragment != "" {
		return errors.New("source URL must not contain a fragment")
	}
	return nil
}

func decodeDownload(request *http.Request, dest any) error {
	defer request.Body.Close()
	data, err := io.ReadAll(io.LimitReader(request.Body, maxDownloadBody+1))
	if err != nil {
		return err
	}
	if len(data) > maxDownloadBody {
		return errors.New("request body is too large")
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return errors.New("request body is required")
	}
	if err := json.Unmarshal(data, dest); err != nil {
		return errors.New("invalid json")
	}
	return nil
}
