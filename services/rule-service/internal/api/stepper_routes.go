package api

import "github.com/go-chi/chi/v5"

func (h *Handler) RegisterStepperRoutes(r chi.Router) {
	r.Route("/api", func(r chi.Router) {
		r.Get("/rules/catalog", h.handleRuleCatalog)
		r.Post("/rules/baseline/check", h.handleRuleBaselineCheck)
		r.Post("/rules/preview", h.handleRulePreview)
		r.Route("/rules", func(r chi.Router) {
			r.Post("/", h.handleStepperRuleCreate)
			r.Put("/{ruleId}", h.handleStepperRuleUpdate)
			r.Get("/", h.handleStepperRuleList)
			r.Delete("/{ruleId}", h.handleStepperRuleDelete)
			r.Post("/{ruleId}/enable", h.handleStepperRuleEnable)
			r.Post("/{ruleId}/disable", h.handleStepperRuleDisable)
		})
		r.Get("/machine-units/{unitId}/parameters", h.handleUnitParameters)
		r.Get("/machine-units/{unitId}/rule-health", h.handleRuleHealth)
	})
}
