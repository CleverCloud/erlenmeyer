package prom

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/prometheus/prometheus/pkg/labels"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	queryPromql "github.com/ovh/erlenmeyer/proto/prom/promql"

	"github.com/ovh/erlenmeyer/core"
)

// processMatchers processes a list of matchers and returns the class name and labels
func processMatchers(matchers []*labels.Matcher) (string, map[string]string) {
	className := ""
	labels := make(map[string]string)

	for _, matcher := range matchers {
		if matcher.Name == "__name__" {
			className = matcher.Value
			continue
		}

		labelsValue := matcher.Value
		switch matcher.Type.String() {
		case "=~":
			labelsValue = "~" + labelsValue
		case "!=", "!~":
			labelsValue = fmt.Sprintf("~(?!%s).*", labelsValue)
		}
		labels[matcher.Name] = labelsValue
	}

	return className, labels
}

// FindSeries returns the list of time series that match a certain label set.
func (p *QL) FindSeries(w http.ResponseWriter, r *http.Request) {
	token := core.RetrieveToken(r)
	if len(token) == 0 {
		respondWithError(w, errors.New("unauthorized, please provide a READ token"), http.StatusForbidden)
		return
	}

	r.ParseForm()
	if len(r.Form["match[]"]) == 0 {
		respondWithError(w, errors.New("no match[] parameter provided"), http.StatusUnprocessableEntity)
		return
	}

	resp := []map[string]string{}

	for _, s := range r.Form["match[]"] {
		matchers, err := queryPromql.ParseMetricSelector(s)
		if err != nil {
			respondWithError(w, err, http.StatusUnprocessableEntity)
			return
		}

		className, labels := processMatchers(matchers)

		findQuery := buildWarp10Selector(className, labels)
		warpServer := core.NewWarpServer(viper.GetString("warp_endpoint"), "prometheus-find")
		gtss, err := warpServer.FindGTS(token, findQuery.String(), time.Time{})

		if err != nil {
			log.WithFields(log.Fields{
				"query": findQuery.String(),
				"error": err.Error(),
			}).Error("Error finding some GTS")
			respondWithError(w, err, http.StatusInternalServerError)
			return
		}

		for _, gts := range gtss.GTS {
			data := make(map[string]string)
			data["__name__"] = gts.Class
			for key, value := range gts.Labels {
				if key == ".app" {
					continue
				}
				data[key] = value
			}
			for key, value := range gts.Attrs {
				data[key] = value
			}
			resp = append(resp, data)
		}
	}
	respondFind(w, resp)
}

type prometheusFindResponse struct {
	Status status              `json:"status"`
	Data   []map[string]string `json:"data"`
}

type prometheusFindLabelsResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

// FindLabelsValues is handling finding labels values
func (p *QL) FindLabelsValues(ctx echo.Context) error {
	w := ctx.Response()
	r := ctx.Request()

	token := core.RetrieveToken(r)
	if len(token) == 0 {
		respondWithError(w, errors.New("unauthorized, please provide a READ token"), http.StatusForbidden)
		return nil
	}

	labelValue := ctx.Param("label")
	if len(labelValue) == 0 {
		log.Error("missing label")
		respondWithError(w, errors.New("unprocessable Entity: label"), http.StatusBadRequest)
		return nil
	}

	// Parse form data
	if err := r.ParseForm(); err != nil {
		respondWithError(w, errors.New("failed to parse form data"), http.StatusBadRequest)
		return nil
	}

	// Get matchers and process them
	matchers := r.Form["match[]"]
	if len(matchers) == 0 {
		// Grafana will try to get all class name when arriving explore page
		// This prevent showing an error to the customer, while allowing to prevent performance
		// bottleneck where the user is fetching 1M series
		resp := prometheusFindLabelsResponse{
			Status: "success",
			Data:   []string{},
		}
		return ctx.JSON(http.StatusOK, resp)
	}

	// TODO: Prevent issues by parsing the start param

	// Process the first matcher to build the Warp10 selector
	matcherObjs, err := queryPromql.ParseMetricSelector(matchers[0])
	if err != nil {
		respondWithError(w, err, http.StatusBadRequest)
		return nil
	}

	// Extract class name and build labels map
	classname, labels := processMatchers(matcherObjs)

	// Build the Warp10 selector
	findQuery := buildWarp10Selector(classname, labels)
	warpServer := core.NewWarpServer(viper.GetString("warp_endpoint"), "prometheus-find-labels")

	// Execute the query
	gtss, err := warpServer.FindGTS(token, findQuery.String(), time.Time{})
	if err != nil {
		log.WithFields(log.Fields{
			"query": findQuery.String(),
			"error": err.Error(),
		}).Error("Error finding some GTS")
		respondWithError(w, err, http.StatusInternalServerError)
		return nil
	}

	// Process results
	var resp prometheusFindLabelsResponse
	resp.Status = "success"

	for _, gts := range gtss.GTS {
		if labelValue == "__name__" {
			resp.Data = append(resp.Data, gts.Class)
		} else if value, exists := gts.Labels[labelValue]; exists {
			resp.Data = append(resp.Data, value)
		}
	}

	resp.Data = unique(resp.Data)
	b, _ := json.Marshal(resp)
	w.Write(b)

	return nil
}

// FindLabels returns all label names for a series
func (p *QL) FindLabels(ctx echo.Context) error {
	w := ctx.Response()
	r := ctx.Request()

	token := core.RetrieveToken(r)
	if len(token) == 0 {
		respondWithError(w, errors.New("unauthorized, please provide a READ token"), http.StatusForbidden)
		return nil
	}

	// Parse form data
	if err := r.ParseForm(); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{
			"error": "failed to parse form data",
		})
	}

	// Get matchers
	matchers := r.Form["match[]"]
	if len(matchers) == 0 {
		// Grafana will try to get all class name when arriving explore page
		// This prevent showing an error to the customer, while allowing to prevent performance
		// bottleneck where the user is fetching 1M series
		resp := prometheusFindLabelsResponse{
			Status: "success",
			Data:   []string{},
		}
		return ctx.JSON(http.StatusOK, resp)
	}

	// Build and execute query
	warpServer := core.NewWarpServer(viper.GetString("warp_endpoint"), "prometheus-find-labels")

	// Store unique labels
	labelSet := make(map[string]struct{})

	for _, matcher := range matchers {
		matcherObjs, err := queryPromql.ParseMetricSelector(matcher)
		if err != nil {
			return ctx.JSON(http.StatusBadRequest, map[string]string{
				"error": fmt.Sprintf("invalid matcher format: %v", err),
			})
		}

		className, labels := processMatchers(matcherObjs)
		findQuery := buildWarp10Selector(className, labels)
		gtss, err := warpServer.FindGTS(token, findQuery.String(), time.Time{})
		if err != nil {
			log.WithFields(log.Fields{
				"query": findQuery.String(),
				"error": err.Error(),
			}).Error("Error finding GTS")
			return ctx.JSON(http.StatusInternalServerError, map[string]string{
				"error": "internal server error while searching for series",
			})
		}

		// Add __name__ label
		labelSet["__name__"] = struct{}{}

		// Collect all unique label names
		for _, gts := range gtss.GTS {
			for key := range gts.Labels {
				if key != ".app" { // Skip internal labels
					labelSet[key] = struct{}{}
				}
			}
			for key := range gts.Attrs {
				labelSet[key] = struct{}{}
			}
		}
	}

	// Convert set to slice
	labels := make([]string, 0, len(labelSet))
	for label := range labelSet {
		labels = append(labels, label)
	}

	// Return response
	resp := prometheusFindLabelsResponse{
		Status: "success",
		Data:   labels,
	}

	return ctx.JSON(http.StatusOK, resp)
}

// FindClassnames handles searching for class names based on matchers
func (p *QL) FindClassnames(ctx echo.Context) error {
	w := ctx.Response()
	r := ctx.Request()

	token := core.RetrieveToken(r)
	if len(token) == 0 {
		respondWithError(w, errors.New("unauthorized, please provide a READ token"), http.StatusForbidden)
		return nil
	}

	// Parse query parameters
	matchers := r.URL.Query()["match[]"]
	if len(matchers) == 0 {

		// Grafana will try to get all class name when arriving explore page
		// This prevent showing an error to the customer, while allowing to prevent performance
		// bottleneck where the user is fetching 1M series

		resp := prometheusFindLabelsResponse{
			Status: "success",
			Data:   []string{},
		}
		return ctx.JSON(http.StatusOK, resp)
	}

	// Process each matcher
	for _, matcher := range matchers {
		// Parse the matcher
		matcherObjs, err := queryPromql.ParseMetricSelector(matcher)
		if err != nil {
			return ctx.JSON(http.StatusBadRequest, map[string]string{
				"error": fmt.Sprintf("invalid matcher format: %v", err),
			})
		}

		// Look for __name__ matcher
		hasNameMatcher := false
		for _, m := range matcherObjs {
			if m.Name == "__name__" {
				hasNameMatcher = true
				// Grafana will add .* suffix and prefix to the value, but the real minimal length is 3 chars
				if len(strings.TrimSpace(fmt.Sprintf("%v", m.Value))) < 7 {
					return ctx.JSON(http.StatusBadRequest, map[string]string{
						"error": "Search must contain at least 3 characters",
					})
				}
			}
		}

		if !hasNameMatcher {
			return ctx.JSON(http.StatusBadRequest, map[string]string{
				"error": "query must include a matcher for __name__",
			})
		}
	}

	// Get time parameters
	startTime := time.Time{}
	if ctx.QueryParam("start") != "" {
		var err error
		startTimeSec, err := strconv.ParseInt(ctx.QueryParam("start"), 10, 64)
		startTime = time.Unix(startTimeSec, 0)
		if err != nil {
			log.WithError(err).Error("Failed to parse start time")
			return ctx.JSON(http.StatusBadRequest, map[string]string{
				"error": "failed to parse start time",
			})
		}
	}

	// Build and execute query
	warpServer := core.NewWarpServer(viper.GetString("warp_endpoint"), "prometheus-find-label-name-values")

	var resp prometheusFindLabelsResponse
	resp.Status = "success"
	resp.Data = []string{}

	for _, matcher := range matchers {
		matcherObjs, _ := queryPromql.ParseMetricSelector(matcher)

		className, labels := processMatchers(matcherObjs)

		uriLabel := ctx.Param("label")
		if uriLabel != "" && uriLabel != "__name__" {
			labels[uriLabel] = "~.*"
		}

		findQuery := buildWarp10Selector(className, labels)
		// We want to do a regex search by default to match series names
		selector := "~" + findQuery.String()
		gtss, err := warpServer.FindGTS(token, selector, startTime)
		if err != nil {
			log.WithFields(log.Fields{
				"query": selector,
				"error": err.Error(),
			}).Error("Error finding GTS")
			return ctx.JSON(http.StatusInternalServerError, map[string]string{
				"error": "internal server error while searching for series",
			})
		}

		for _, gts := range gtss.GTS {
			resp.Data = append(resp.Data, gts.Class)
		}
	}

	resp.Data = unique(resp.Data)
	return ctx.JSON(http.StatusOK, resp)
}

// FindAndDeleteSeries is handling /find and /delete for series
func (p *QL) FindAndDeleteSeries(w http.ResponseWriter, r *http.Request) {

	switch r.Method {
	case "DELETE":
		p.Delete(w, r)
	case "GET":
		p.FindSeries(w, r)
	}
}
