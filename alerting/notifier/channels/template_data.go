package channels

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/alertmanager/notify"
	amtemplate "github.com/prometheus/alertmanager/template"
	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/common/model"

	"github.com/grafana/alerting/alerting/models"
	"github.com/grafana/alerting/utils"
)

var (
	ErrTemplateOutputTooLarge = errors.New("template output exceeds maximum size")
	MaxTemplateOutputSize     = int64(10 * 1024 * 1024) // 10MB
)

type ExtendedAlert struct {
	Status        string             `json:"status"`
	Labels        amtemplate.KV      `json:"labels"`
	Annotations   amtemplate.KV      `json:"annotations"`
	StartsAt      time.Time          `json:"startsAt"`
	EndsAt        time.Time          `json:"endsAt"`
	GeneratorURL  string             `json:"generatorURL"`
	Fingerprint   string             `json:"fingerprint"`
	SilenceURL    string             `json:"silenceURL"`
	DashboardURL  string             `json:"dashboardURL"`
	PanelURL      string             `json:"panelURL"`
	Values        map[string]float64 `json:"values"`
	ValueString   string             `json:"valueString"` // TODO: Remove in Grafana 10
	ImageURL      string             `json:"imageURL,omitempty"`
	EmbeddedImage string             `json:"embeddedImage,omitempty"`
}

type ExtendedAlerts []ExtendedAlert

type ExtendedData struct {
	Receiver string         `json:"receiver"`
	Status   string         `json:"status"`
	Alerts   ExtendedAlerts `json:"alerts"`

	GroupLabels       amtemplate.KV `json:"groupLabels"`
	CommonLabels      amtemplate.KV `json:"commonLabels"`
	CommonAnnotations amtemplate.KV `json:"commonAnnotations"`

	ExternalURL string `json:"externalURL"`
}

func removePrivateItems(kv amtemplate.KV) amtemplate.KV {
	for key := range kv {
		if strings.HasPrefix(key, "__") && strings.HasSuffix(key, "__") {
			kv = kv.Remove([]string{key})
		}
	}
	return kv
}

func extendAlert(alert amtemplate.Alert, externalURL string, logger Logger) *ExtendedAlert {
	// remove "private" annotations & labels so they don't show up in the template
	extended := &ExtendedAlert{
		Status:       alert.Status,
		Labels:       removePrivateItems(alert.Labels),
		Annotations:  removePrivateItems(alert.Annotations),
		StartsAt:     alert.StartsAt,
		EndsAt:       alert.EndsAt,
		GeneratorURL: alert.GeneratorURL,
		Fingerprint:  alert.Fingerprint,
	}

	// fill in some grafana-specific urls
	if len(externalURL) == 0 {
		return extended
	}
	u, err := url.Parse(externalURL)
	if err != nil {
		logger.Debug("failed to parse external URL while extending template data", "url", externalURL, "error", err.Error())
		return extended
	}
	externalPath := u.Path
	dashboardUID := alert.Annotations[models.DashboardUIDAnnotation]
	if len(dashboardUID) > 0 {
		u.Path = path.Join(externalPath, "/d/", dashboardUID)
		extended.DashboardURL = u.String()
		panelID := alert.Annotations[models.PanelIDAnnotation]
		if len(panelID) > 0 {
			u.RawQuery = "viewPanel=" + panelID
			extended.PanelURL = u.String()
		}

		generatorURL, err := url.Parse(extended.GeneratorURL)
		if err != nil {
			logger.Debug("failed to parse generator URL while extending template data", "url", extended.GeneratorURL, "err", err.Error())
			return extended
		}

		dashboardURL, err := url.Parse(extended.DashboardURL)
		if err != nil {
			logger.Debug("failed to parse dashboard URL while extending template data", "url", extended.DashboardURL, "err", err.Error())
			return extended
		}

		orgID := alert.Annotations[models.OrgIDAnnotation]
		if len(orgID) > 0 {
			extended.DashboardURL = setOrgIDQueryParam(dashboardURL, orgID)
			extended.PanelURL = setOrgIDQueryParam(u, orgID)
			extended.GeneratorURL = setOrgIDQueryParam(generatorURL, orgID)
		}
	}

	if alert.Annotations != nil {
		if s, ok := alert.Annotations[models.ValuesAnnotation]; ok {
			if err := json.Unmarshal([]byte(s), &extended.Values); err != nil {
				logger.Warn("failed to unmarshal values annotation", "error", err)
			}
		}

		// TODO: Remove in Grafana 10
		extended.ValueString = alert.Annotations[models.ValueStringAnnotation]
	}

	matchers := make([]string, 0)
	for key, value := range alert.Labels {
		if !(strings.HasPrefix(key, "__") && strings.HasSuffix(key, "__")) {
			matchers = append(matchers, key+"="+value)
		}
	}
	sort.Strings(matchers)
	u.Path = path.Join(externalPath, "/alerting/silence/new")

	query := make(url.Values)
	query.Add("alertmanager", "grafana")
	for _, matcher := range matchers {
		query.Add("matcher", matcher)
	}

	u.RawQuery = query.Encode()

	extended.SilenceURL = u.String()

	return extended
}

func setOrgIDQueryParam(url *url.URL, orgID string) string {
	q := url.Query()
	q.Set("orgId", orgID)
	url.RawQuery = q.Encode()

	return url.String()
}

func ExtendData(data *amtemplate.Data, logger Logger) *ExtendedData {
	alerts := []ExtendedAlert{}

	for _, alert := range data.Alerts {
		extendedAlert := extendAlert(alert, data.ExternalURL, logger)
		alerts = append(alerts, *extendedAlert)
	}

	extended := &ExtendedData{
		Receiver:          data.Receiver,
		Status:            data.Status,
		Alerts:            alerts,
		GroupLabels:       data.GroupLabels,
		CommonLabels:      removePrivateItems(data.CommonLabels),
		CommonAnnotations: removePrivateItems(data.CommonAnnotations),

		ExternalURL: data.ExternalURL,
	}
	return extended
}

func TmplText(ctx context.Context, tmpl *amtemplate.Template, alerts []*types.Alert, l Logger, tmplErr *error) (func(string) string, *ExtendedData) {
	promTmplData := notify.GetTemplateData(ctx, tmpl, alerts, l)
	data := ExtendData(promTmplData, l)

	return func(name string) (s string) {
		if *tmplErr != nil {
			return
		}
		s, *tmplErr = executeTextStringWithLimit(tmpl, name, data)
		return s
	}, data
}

func executeTextStringWithLimit(tmpl *amtemplate.Template, name string, data *ExtendedData) (string, error) {
	result, err := tmpl.ExecuteTextString(name, data)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	_, writeErr := utils.NewLimitedWriter(&buf, MaxTemplateOutputSize).Write([]byte(result))
	if errors.Is(writeErr, utils.ErrWriteLimitExceeded) {
		return "", ErrTemplateOutputTooLarge
	}

	return result, nil
}

// Firing returns the subset of alerts that are firing.
func (as ExtendedAlerts) Firing() []ExtendedAlert {
	res := []ExtendedAlert{}
	for _, a := range as {
		if a.Status == string(model.AlertFiring) {
			res = append(res, a)
		}
	}
	return res
}

// Resolved returns the subset of alerts that are resolved.
func (as ExtendedAlerts) Resolved() []ExtendedAlert {
	res := []ExtendedAlert{}
	for _, a := range as {
		if a.Status == string(model.AlertResolved) {
			res = append(res, a)
		}
	}
	return res
}
