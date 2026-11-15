package timectl

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"example.com/dstate/internal/control"
)

// Routes returns the operator API of the time controller, as pattern to
// handler:
//
//	POST /time       {"time": "2026-11-15T12:00:00Z"}   set the time by hand
//	POST /time/mode  {"mode": "auto"}                   go back to auto mode
//
// submit commits an event from this node. A request is answered once its
// event is committed; whether the controller acts on it depends on its state
// (the time can only be set in internal mode), which GET /state shows.
func Routes(submit func(context.Context, control.Event) error) map[string]http.HandlerFunc {
	commit := func(w http.ResponseWriter, r *http.Request, name string, params map[string]any) {
		if err := submit(r.Context(), control.Event{Target: Name, Name: name, Params: params}); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(params)
	}
	return map[string]http.HandlerFunc{
		"POST /time": func(w http.ResponseWriter, r *http.Request) {
			// The clock is read here, once, and travels with the event: the
			// controller computes the delta from these two numbers on every
			// replica alike.
			now := time.Now()
			var req struct {
				Time time.Time `json:"time"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Time.IsZero() {
				http.Error(w, `expected {"time": "<RFC 3339 time>"}`, http.StatusBadRequest)
				return
			}
			commit(w, r, EvSetTime, map[string]any{
				"now_ms":   now.UnixMilli(),
				"time_ms":  req.Time.UnixMilli(),
				"delta_ms": now.UnixMilli() - req.Time.UnixMilli(),
			})
		},
		"POST /time/mode": func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				Mode string `json:"mode"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Mode != ModeAuto {
				http.Error(w, `expected {"mode": "auto"}`, http.StatusBadRequest)
				return
			}
			commit(w, r, EvUseAuto, map[string]any{"mode": req.Mode})
		},
	}
}
