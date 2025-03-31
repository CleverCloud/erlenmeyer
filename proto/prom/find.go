package prom

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

const (
	DEFAULT_METRIC_SELECTOR        = "(http|prometheus).*"
	DEFAULT_METRIC_SELECTOR_GCOUNT = 100
	MAX_GCOUNT_PER_FIND            = 200
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
		respondWithError(w, errors.New("please provide a READ token"), http.StatusUnauthorized)
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
		gtss, err := warpServer.FindGTS(token, findQuery.String(), core.FindParameters{})

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

// prometheusSeriesResponse represents the response format for the /api/v1/series endpoint
type prometheusSeriesResponse struct {
	Status string              `json:"status"`
	Data   []map[string]string `json:"data"`
}

// FindLabelsValues is handling finding labels values
func (p *QL) FindLabelsValues(ctx echo.Context) error {
	w := ctx.Response()
	r := ctx.Request()

	token := core.RetrieveToken(r)
	if len(token) == 0 {
		respondWithError(w, errors.New("please provide a READ token"), http.StatusUnauthorized)
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
	gtss, err := warpServer.FindGTS(token, findQuery.String(), core.FindParameters{})
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
		respondWithError(w, errors.New("please provide a READ token"), http.StatusUnauthorized)
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
		gtss, err := warpServer.FindGTS(token, findQuery.String(), core.FindParameters{})
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
	labels := make([]string, len(labelSet))
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

// FindClassnamesHandler is the Echo handler for the /api/v1/label/__name__/values endpoint
func (p *QL) FindClassnamesHandler(ctx echo.Context) error {
	w := ctx.Response()
	r := ctx.Request()
	resp := prometheusFindLabelsResponse{
		Status: "success",
		Data:   []string{},
	}

	// Extract token
	token := core.RetrieveToken(r)
	if len(token) == 0 {
		respondWithError(w, errors.New("please provide a READ token"), http.StatusUnauthorized)
		return nil
	}

	// Parse query parameters
	matchers := r.URL.Query()["match[]"]

	// Get time parameters
	startTime := time.Time{}
	if ctx.QueryParam("start") != "" {
		var err error
		startTimeSec, err := strconv.ParseInt(ctx.QueryParam("start"), 10, 64)
		if err != nil {
			log.WithError(err).Error("Failed to parse start time")
			return ctx.JSON(http.StatusBadRequest, map[string]string{
				"error": "failed to parse start time",
			})
		}
		startTime = time.Unix(startTimeSec, 0)

		// Apply time range limits
		startTime = applyTimeRangeLimits(startTime)
	}

	// Get label parameter from URI
	uriLabel := ctx.Param("label")

	// Call the core function
	series, statusCode, err := p.FindClassnames(token, matchers, startTime, uriLabel)
	if err != nil {
		return ctx.JSON(statusCode, map[string]string{
			"error": err.Error(),
		})
	}

	containsString := func(slice []string, str string) bool {
		for _, item := range slice {
			if item == str {
				return true
			}
		}
		return false
	}

	for _, series := range series {
		if !containsString(resp.Data, series.Class) {
			resp.Data = append(resp.Data, series.Class)
		}
	}

	return ctx.JSON(http.StatusOK, resp)
}

// applyTimeRangeLimits enforces the minimum and maximum time range for the series endpoint
func applyTimeRangeLimits(startTime time.Time) time.Time {
	// Get the configured minimum and maximum time ranges
	minTimeRangeStr := viper.GetString("warp10.find.activeafter.min")
	maxTimeRangeStr := viper.GetString("warp10.find.activeafter.max")

	// Parse the minimum time range
	minDuration, err := time.ParseDuration(minTimeRangeStr)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
			"value": minTimeRangeStr,
		}).Warn("Failed to parse minimum time range, using default 24h")
		minDuration = 24 * time.Hour
	}

	// Parse the maximum time range
	maxDuration, err := time.ParseDuration(maxTimeRangeStr)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
			"value": maxTimeRangeStr,
		}).Warn("Failed to parse maximum time range, using default 7d")
		maxDuration = 7 * 24 * time.Hour
	}

	// Calculate the minimum allowed start time (now - maxDuration)
	minAllowedTime := time.Now().Add(-maxDuration)

	// Calculate the maximum allowed start time (now - minDuration)
	maxAllowedTime := time.Now().Add(-minDuration)

	// Apply the limits
	if startTime.Before(minAllowedTime) {
		log.WithFields(log.Fields{
			"requested": startTime,
			"adjusted":  minAllowedTime,
		}).Info("Adjusted start time to minimum allowed value")
		return minAllowedTime
	}

	if startTime.After(maxAllowedTime) {
		log.WithFields(log.Fields{
			"requested": startTime,
			"adjusted":  maxAllowedTime,
		}).Info("Adjusted start time to maximum allowed value")
		return maxAllowedTime
	}

	return startTime
}

// FindClassnames handles searching for class names based on matchers using primitive parameters
func (p *QL) FindClassnames(token string, matchers []string, startTime time.Time, uriLabel string) ([]core.GeoTimeSeries, int, error) {
	var resp []core.GeoTimeSeries
	params := core.FindParameters{}

	// If no matchers provided, we run a simple request with a low limit to prevent
	// performance issues & long running requests
	if len(matchers) == 0 {
		matchers = append(matchers, "{__name__=~'"+DEFAULT_METRIC_SELECTOR+"'}")
		params.GCount = DEFAULT_METRIC_SELECTOR_GCOUNT
	}

	// Process each matcher
	for _, matcher := range matchers {
		// Parse the matcher
		matcherObjs, err := queryPromql.ParseMetricSelector(matcher)
		if err != nil {
			return resp, http.StatusBadRequest, fmt.Errorf("invalid matcher format: %v", err)
		}

		// Look for __name__ matcher
		hasNameMatcher := false
		for _, m := range matcherObjs {
			if m.Name == "__name__" {
				hasNameMatcher = true
				if m.Value == DEFAULT_METRIC_SELECTOR {
					continue
				}

				if len(strings.TrimSpace(fmt.Sprintf("%v", m.Value))) < 7 {
					return resp, http.StatusBadRequest, fmt.Errorf("search must contain at least 3 characters")
				}
			}
		}

		if !hasNameMatcher {
			return resp, http.StatusBadRequest, fmt.Errorf("query must include a matcher for __name__")
		}
	}

	// Build and execute query
	warpServer := core.NewWarpServer(viper.GetString("warp_endpoint"), "prometheus-find-label-name-values")

	for _, matcher := range matchers {
		matcherObjs, _ := queryPromql.ParseMetricSelector(matcher)

		className, labels := processMatchers(matcherObjs)

		if uriLabel != "" && uriLabel != "__name__" {
			labels[uriLabel] = "~.*"
		}

		findQuery := buildWarp10Selector(className, labels)
		// We want to do a regex search by default to match series names
		selector := "~" + findQuery.String()
		gtss, err := warpServer.FindGTS(token, selector, params)
		if err != nil {
			log.WithFields(log.Fields{
				"query": selector,
				"error": err.Error(),
			}).Error("Error finding GTS")
			return resp, http.StatusInternalServerError, fmt.Errorf("internal server error while searching for series")
		}

		resp = append(resp, gtss.GTS...)
	}

	return resp, http.StatusOK, nil
}

// FindAndDeleteSeries is handling /find and /delete for series
func (p *QL) FindAndDeleteSeries(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "DELETE":
		p.Delete(w, r)
	case "GET":
		p.FindSeries(w, r)
	case "POST":
		p.handleSeriesPost(w, r)
	}
}

func (p *QL) handleSeriesPost(w http.ResponseWriter, r *http.Request) {
	// Log the request body for debugging
	body, err := io.ReadAll(r.Body)
	if err != nil {
		respondWithError(w, fmt.Errorf("failed to read request body: %v", err), http.StatusBadRequest)
		return
	}

	// Create a new reader from the body so it can be read again
	r.Body = io.NopCloser(bytes.NewBuffer(body))

	// Parse the form data from the request body
	if err := r.ParseForm(); err != nil {
		respondWithError(w, fmt.Errorf("failed to parse form data: %v", err), http.StatusBadRequest)
		return
	}

	// Get matchers from the form data
	matchers := r.Form["match[]"]
	if len(matchers) == 0 {
		respondWithError(w, errors.New("no match[] parameter provided"), http.StatusBadRequest)
		return
	}

	// Extract token
	token := core.RetrieveToken(r)
	if len(token) == 0 {
		respondWithError(w, errors.New("please provide a READ token"), http.StatusUnauthorized)
		return
	}

	// Parse start time
	startTime := time.Time{}
	if startStr := r.FormValue("start"); startStr != "" {
		startTimeSec, err := strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			respondWithError(w, fmt.Errorf("failed to parse start time: %v", err), http.StatusBadRequest)
			return
		}
		startTime = time.Unix(startTimeSec, 0)

		// Apply time range limits
		startTime = applyTimeRangeLimits(startTime)

		// Log the adjusted time for debugging
		log.WithFields(log.Fields{
			"original_start": time.Unix(startTimeSec, 0),
			"adjusted_start": startTime,
		}).Debug("Applied time range limits to series request")
	}

	// Call the core FindClassnames function directly with primitive parameters
	series, statusCode, err := p.FindClassnames(token, matchers, startTime, "")
	if err != nil {
		respondWithError(w, err, statusCode)
		return
	}

	// Create the Prometheus response format
	seriesResp := prometheusSeriesResponse{
		Status: "success",
		Data:   make([]map[string]string, 0, len(series)),
	}

	// Transform each GeoTimeSeries to the expected Prometheus format
	for _, gts := range series {
		// Create a map for this series with all labels
		seriesLabels := make(map[string]string)

		// Add the class name as __name__ label
		seriesLabels["__name__"] = gts.Class

		// Add all other labels
		for labelName, labelValue := range gts.Labels {
			seriesLabels[labelName] = labelValue
		}

		// Add the series to the response
		seriesResp.Data = append(seriesResp.Data, seriesLabels)
	}

	// Return the JSON response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(seriesResp)
}
