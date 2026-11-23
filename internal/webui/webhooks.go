package webui

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/GerhardOfRivia/bezorg/internal/webhook"
)

const maxWebhookBody = 8 << 10

type webhookRequest struct {
	URL         string   `json:"url"`
	Secret      string   `json:"secret"`
	Events      []string `json:"events"`
	MaxAttempts *int     `json:"max_attempts"`
}

type webhooksResponse struct {
	Webhooks []webhook.Hook `json:"webhooks"`
}

type webhookDeliveriesResponse struct {
	Deliveries []webhook.Delivery `json:"deliveries"`
}

type deletedWebhookResponse struct {
	ID int64 `json:"id"`
}

func (api apiServer) createWebhook(output http.ResponseWriter, request *http.Request) {
	var body webhookRequest
	if err := decodeWebhook(request, &body); err != nil {
		writeAPIError(output, http.StatusBadRequest, err.Error())
		return
	}
	attempts := 0
	if body.MaxAttempts != nil {
		attempts = *body.MaxAttempts
	}
	hook, err := api.svc.CreateWebhook(request.Context(), webhook.Params{
		URL: body.URL, Secret: body.Secret, Events: body.Events, MaxAttempts: attempts,
	})
	if err != nil {
		writeWebhookError(output, err)
		return
	}
	writeJSON(output, http.StatusCreated, hook)
}

func (api apiServer) listWebhooks(output http.ResponseWriter, request *http.Request) {
	hooks, err := api.svc.ListWebhooks(request.Context())
	if err != nil {
		writeWebhookError(output, err)
		return
	}
	if hooks == nil {
		hooks = []webhook.Hook{}
	}
	writeJSON(output, http.StatusOK, webhooksResponse{Webhooks: hooks})
}

func (api apiServer) deleteWebhook(output http.ResponseWriter, request *http.Request) {
	id, err := webhookID(request.PathValue("id"))
	if err != nil {
		writeAPIError(output, http.StatusBadRequest, err.Error())
		return
	}
	if err := api.svc.DeleteWebhook(request.Context(), id); err != nil {
		writeWebhookError(output, err)
		return
	}
	writeJSON(output, http.StatusOK, deletedWebhookResponse{ID: id})
}

func (api apiServer) webhookDeliveries(output http.ResponseWriter, request *http.Request) {
	id, err := webhookID(request.PathValue("id"))
	if err != nil {
		writeAPIError(output, http.StatusBadRequest, err.Error())
		return
	}
	status := request.URL.Query().Get("status")
	switch status {
	case "", "pending", "succeeded", "dead":
	default:
		writeAPIError(output, http.StatusBadRequest, "status must be pending, succeeded, or dead")
		return
	}
	deliveries, err := api.svc.ListWebhookDeliveries(request.Context(), id, status, 100)
	if err != nil {
		writeWebhookError(output, err)
		return
	}
	if deliveries == nil {
		deliveries = []webhook.Delivery{}
	}
	writeJSON(output, http.StatusOK, webhookDeliveriesResponse{Deliveries: deliveries})
}

func decodeWebhook(request *http.Request, dest any) error {
	defer request.Body.Close()
	data, err := io.ReadAll(io.LimitReader(request.Body, maxWebhookBody+1))
	if err != nil {
		return err
	}
	if len(data) > maxWebhookBody {
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

func webhookID(raw string) (int64, error) {
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return 0, errors.New("webhook id must be a positive integer")
	}
	return id, nil
}

func writeWebhookError(output http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, webhook.ErrNotFound):
		writeAPIError(output, http.StatusNotFound, "webhook not found")
	case errors.Is(err, webhook.ErrLimit):
		writeAPIError(output, http.StatusConflict, err.Error())
	default:
		message := err.Error()
		if strings.HasPrefix(message, "webhook:") {
			writeAPIError(output, http.StatusBadRequest, message)
			return
		}
		writeAPIError(output, http.StatusServiceUnavailable, message)
	}
}
