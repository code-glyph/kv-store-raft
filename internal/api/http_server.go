package api

import "net/http"

func RegisterHandlers(mux *http.ServeMux, h *Handler) {
	mux.HandleFunc("/healthz", h.HandleHealth)
	mux.HandleFunc("/leader", h.HandleLeader)
	mux.HandleFunc("/kv/", h.HandleKV)
}
