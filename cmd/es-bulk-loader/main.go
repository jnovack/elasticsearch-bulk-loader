package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/jnovack/flag"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var version = "dev" // default if not overridden

type bulkResponse struct {
	Errors bool                          `json:"errors"`
	Items  []map[string]bulkItemResponse `json:"items"`
}

type bulkItemResponse struct {
	Index  string         `json:"_index"`
	ID     string         `json:"_id"`
	Status int            `json:"status"`
	Error  *bulkItemError `json:"error,omitempty"`
}

type bulkItemError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

type bulkInsertResult struct {
	Succeeded int
	Failed    int
}

type enrichFlagValue struct {
	enabled bool
	all     bool
	raw     string
}

type enrichPolicySummary struct {
	Config map[string]struct {
		Name string `json:"name"`
	} `json:"config"`
}

type enrichPoliciesResponse struct {
	Policies []enrichPolicySummary `json:"policies"`
}

type enrichExecuteResponse struct {
	Status *struct {
		Phase string `json:"phase"`
	} `json:"status,omitempty"`
	Task *string `json:"task,omitempty"`
}

func (e *enrichFlagValue) String() string {
	if e == nil {
		return ""
	}
	if e.all {
		return "all"
	}
	return e.raw
}

func (e *enrichFlagValue) Set(value string) error {
	e.enabled = true
	trimmed := strings.TrimSpace(value)
	switch trimmed {
	case "", "true":
		e.all = true
		e.raw = ""
	case "false":
		e.enabled = false
		e.all = false
		e.raw = ""
	default:
		e.all = false
		e.raw = trimmed
	}
	return nil
}

func (e *enrichFlagValue) IsBoolFlag() bool {
	return true
}

func (e *enrichFlagValue) explicitPolicies() []string {
	if e == nil || !e.enabled || e.all {
		return nil
	}

	policies := make([]string, 0)
	seen := make(map[string]struct{})
	for _, policy := range strings.Split(e.raw, ",") {
		name := strings.TrimSpace(policy)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		policies = append(policies, name)
	}
	return policies
}

func main() {
	// CLI flags
	url := flag.String("url", "http://localhost:9200", "Elasticsearch URL")
	insecure := flag.Bool("insecureSkipVerify", false, "Skip TLS verification")
	index := flag.String("index", "", "Elasticsearch index name")
	settingsFile := flag.String("settings", "", "Path to index settings JSON file (optional)")
	mappingsFile := flag.String("mappings", "", "Path to index mappings JSON file (optional)")
	dataFile := flag.String("data", "", "Path to bulk JSON data file (array of objects)")
	batchSize := flag.Int("batch", 1000, "Batch size for bulk inserts")
	deleteIndex := flag.Bool("delete", false, "Delete index if it exists")
	addToIndex := flag.Bool("add", false, "Add documents to existing index")
	flushIndex := flag.Bool("flush", false, "Delete all documents from an existing index without deleting the index")
	idField := flag.String("id", "", "Field to use to override _id (not normal)")
	user := flag.String("user", "", "Username for basic auth (optional)")
	pass := flag.String("pass", "", "Password for basic auth (optional)")
	apiKey := flag.String("apiKey", "", "Elasticsearch API key (optional)")
	enrich := &enrichFlagValue{}
	flag.Var(enrich, "enrich", "Run enrich policies after the bulk insert; provide a comma-separated policy list or omit the value to run all policies")
	showVersion := flag.Bool("version", false, "print version and exit")

	flag.String(flag.DefaultConfigFlagname, "", "path to config file")

	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		os.Exit(0)
	}

	// Init logger
	zerolog.TimeFieldFormat = time.RFC3339
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	log.Info().Str("version", version).Msg("jnovack/es-bulk-loader starting...")

	if (*user != "" || *pass != "") && *apiKey != "" {
		log.Info().Msg("Cannot use both basic auth and API key. Choose one method.")
		flag.Usage()
		os.Exit(1)
	}

	if *index == "" || *dataFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	// Set up Elasticsearch client
	cfg := elasticsearch.Config{
		Addresses: []string{*url},
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: *insecure,
			},
		},
	}

	// Basic Auth
	if *user != "" && *pass != "" {
		cfg.Username = *user
		cfg.Password = *pass
	}

	// API Key
	if *apiKey != "" {
		cfg.APIKey = *apiKey
	}
	es, err := elasticsearch.NewClient(cfg)
	checkErr("creating Elasticsearch client", err)

	// Determine if index exists
	exists, err := indexExists(es, *index)
	checkErr("checking if index exists", err)

	if exists {
		switch {
		case *deleteIndex:
			if *addToIndex {
				log.Info().Str("index", *index).Msg("Deleting and recreating index before adding documents")
			} else {
				log.Info().Str("index", *index).Msg("Deleting index")
			}
			deleteAndCheck(es, *index)
			exists = false
		case *flushIndex:
			if *addToIndex {
				log.Info().Str("index", *index).Msg("Flushing existing index before adding documents")
			} else {
				log.Info().Str("index", *index).Msg("Flushing existing index")
			}
			flushAndCheck(es, *index)
		case *addToIndex:
			log.Info().Str("index", *index).Msg("Appending documents to existing index")
		default:
			log.Fatal().
				Str("index", *index).
				Msg("Index exists. Use -delete to recreate, -flush to clear documents, or -add to append.")
		}
	} else {
		if *deleteIndex {
			log.Warn().Str("index", *index).Msg("Index does not exist. Nothing to delete.")
		}
		if *flushIndex {
			log.Warn().Str("index", *index).Msg("Index does not exist. Nothing to flush.")
		}
		if *addToIndex {
			log.Info().Str("index", *index).Msg("Creating index to append documents")
		} else {
			log.Info().Str("index", *index).Msg("Creating index before loading data")
		}
	}

	// Create index if needed
	if !exists {
		body := buildCreateIndexBody(*settingsFile, *mappingsFile)
		res, err := es.Indices.Create(*index, es.Indices.Create.WithBody(strings.NewReader(body)))
		checkErr("creating index", err)
		defer res.Body.Close()
		log.Info().Str("index", *index).Msg("Index created")
	}

	// Stream data: first pass to count total objects
	f, err := os.Open(*dataFile)
	checkErr("opening data file", err)
	defer f.Close()

	dec := json.NewDecoder(f)
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('[') {
		log.Fatal().Msg("Data file must be a JSON array")
	}

	total := 0
	for dec.More() {
		var tmp map[string]interface{}
		if err := dec.Decode(&tmp); err != nil {
			log.Fatal().Err(err).Msg("Error counting objects in data file")
		}
		total++
	}

	log.Info().Int("total", total).Msg("Starting bulk insert")

	// Second pass: stream and batch insert
	f.Seek(0, 0)
	dec = json.NewDecoder(f)
	_, err = dec.Token() // skip [
	if err != nil {
		log.Fatal().Err(err).Msg("Error re-reading data file")
	}

	overallStart := time.Now()
	batch := make([]map[string]interface{}, 0, *batchSize)
	processed := 0
	succeededTotal := 0
	failedTotal := 0
	for dec.More() {
		var doc map[string]interface{}
		if err := dec.Decode(&doc); err != nil {
			log.Fatal().Err(err).Msg("Error decoding object in data file")
		}
		batch = append(batch, doc)
		if len(batch) == *batchSize {
			result := bulkInsert(es, *index, batch, processed+len(batch), total, *idField)
			processed += len(batch)
			succeededTotal += result.Succeeded
			failedTotal += result.Failed
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		result := bulkInsert(es, *index, batch, processed+len(batch), total, *idField)
		processed += len(batch)
		succeededTotal += result.Succeeded
		failedTotal += result.Failed
	}

	overallDuration := time.Since(overallStart)
	log.Info().
		Int("processed", processed).
		Int("succeeded", succeededTotal).
		Int("failed", failedTotal).
		Float64("total_time", overallDuration.Seconds()).
		Msg("Bulk load completed")

	if failedTotal > 0 {
		log.Warn().
			Int("failed", failedTotal).
			Msg("Bulk load completed with failed items")

		// TODO: document and implement retry strategy for retryable bulk item failures (e.g. 429/503), plus dead-letter handling for non-retryable items
	}

	if enrich.enabled {
		refreshIndex(es, *index)
		runEnrichPolicies(es, enrich)
	}

}

func checkErr(context string, err error) {
	if err != nil {
		log.Fatal().Err(err).Msgf("Error during %s", context)
	}
}

func indexExists(es *elasticsearch.Client, index string) (bool, error) {
	res, err := es.Indices.Exists([]string{index})
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	switch res.StatusCode {
	case 200:
		return true, nil
	case 404:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected status code %d", res.StatusCode)
	}
}

func deleteAndCheck(es *elasticsearch.Client, index string) {
	res, err := es.Indices.Delete([]string{index})
	checkErr("deleting index", err)
	defer res.Body.Close()

	if res.IsError() {
		log.Fatal().Str("index", index).Msg("Failed to delete index")
	}
}

func flushAndCheck(es *elasticsearch.Client, index string) {
	query := `{"query":{"match_all":{}}}`
	res, err := es.DeleteByQuery(
		[]string{index},
		strings.NewReader(query),
		es.DeleteByQuery.WithConflicts("proceed"),
		es.DeleteByQuery.WithRefresh(true),
	)
	checkErr("flushing index", err)
	defer res.Body.Close()

	if res.IsError() {
		log.Fatal().Str("index", index).Msg("Failed to flush index")
	}
}

func buildCreateIndexBody(settingsFile, mappingsFile string) string {
	settings := "{}"
	mappings := "{}"

	if settingsFile != "" {
		if content, err := os.ReadFile(settingsFile); err == nil {
			settings = string(content)
		} else {
			log.Fatal().Err(err).Msg("Reading settings file")
		}
	}

	if mappingsFile != "" {
		if content, err := os.ReadFile(mappingsFile); err == nil {
			mappings = string(content)
		} else {
			log.Fatal().Err(err).Msg("Reading mappings file")
		}
	}

	return fmt.Sprintf(`{"settings": %s, "mappings": %s}`, settings, mappings)
}

func refreshIndex(es *elasticsearch.Client, index string) {
	res, err := es.Indices.Refresh(es.Indices.Refresh.WithIndex(index))
	checkErr("refreshing index before enrich execution", err)
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		log.Fatal().
			Str("index", index).
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Failed to refresh index before enrich execution")
	}

	log.Info().Str("index", index).Msg("Index refreshed before enrich execution")
}

func runEnrichPolicies(es *elasticsearch.Client, enrich *enrichFlagValue) {
	availablePolicies := getEnrichPolicies(es)
	if len(availablePolicies) == 0 {
		log.Info().Msg("No enrich policies found; skipping enrich execution")
		return
	}

	targets, missing := resolveEnrichTargets(enrich, availablePolicies)
	for _, policy := range missing {
		log.Warn().Str("policy", policy).Msg("Enrich policy not found; skipping")
	}

	if len(targets) == 0 {
		log.Warn().Msg("No enrich policies matched the request")
		return
	}

	log.Info().
		Int("available", len(availablePolicies)).
		Int("requested", len(targets)+len(missing)).
		Int("selected", len(targets)).
		Msg("Starting enrich policy execution")

	succeeded := 0
	failed := 0
	for _, policy := range targets {
		if executeEnrichPolicy(es, policy) {
			succeeded++
		} else {
			failed++
		}
	}

	event := log.Info()
	if failed > 0 {
		event = log.Error()
	}
	event.
		Int("selected", len(targets)).
		Int("succeeded", succeeded).
		Int("failed", failed).
		Int("missing", len(missing)).
		Msg("Enrich policy execution completed")
}

func getEnrichPolicies(es *elasticsearch.Client) []string {
	res, err := es.EnrichGetPolicy(es.EnrichGetPolicy.WithContext(context.Background()))
	checkErr("getting enrich policies", err)
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		log.Fatal().
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Failed to get enrich policies")
	}

	var parsed enrichPoliciesResponse
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		log.Fatal().Err(err).Msg("Unable to parse enrich policy response")
	}

	policies := make([]string, 0, len(parsed.Policies))
	for _, policy := range parsed.Policies {
		for _, config := range policy.Config {
			if config.Name == "" {
				continue
			}
			policies = append(policies, config.Name)
		}
	}
	slices.Sort(policies)
	return policies
}

func resolveEnrichTargets(enrich *enrichFlagValue, available []string) ([]string, []string) {
	if enrich == nil || !enrich.enabled {
		return nil, nil
	}

	availableSet := make(map[string]struct{}, len(available))
	for _, policy := range available {
		availableSet[policy] = struct{}{}
	}

	if enrich.all {
		targets := append([]string(nil), available...)
		slices.Sort(targets)
		return targets, nil
	}

	targets := make([]string, 0)
	missing := make([]string, 0)
	for _, policy := range enrich.explicitPolicies() {
		if _, ok := availableSet[policy]; ok {
			targets = append(targets, policy)
			continue
		}
		missing = append(missing, policy)
	}
	return targets, missing
}

func executeEnrichPolicy(es *elasticsearch.Client, policy string) bool {
	startTime := time.Now()
	res, err := es.EnrichExecutePolicy(
		policy,
		es.EnrichExecutePolicy.WithContext(context.Background()),
		es.EnrichExecutePolicy.WithWaitForCompletion(true),
	)
	if err != nil {
		log.Error().Err(err).Str("policy", policy).Msg("Failed to execute enrich policy")
		return false
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		log.Error().
			Str("policy", policy).
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Float64("time_taken", time.Since(startTime).Seconds()).
			Msg("Enrich policy execution failed")
		return false
	}

	var parsed enrichExecuteResponse
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		log.Error().Err(err).Str("policy", policy).Msg("Unable to parse enrich policy execution response")
		return false
	}

	isFailure := false
	event := log.Info()
	phase := ""
	if parsed.Status != nil {
		phase = parsed.Status.Phase
		if strings.EqualFold(phase, "FAILED") || strings.EqualFold(phase, "CANCELLED") {
			isFailure = true
			event = log.Error()
		}
	}

	entry := event.
		Str("policy", policy).
		Float64("time_taken", time.Since(startTime).Seconds())
	if phase != "" {
		entry = entry.Str("phase", phase)
	}
	if parsed.Task != nil && *parsed.Task != "" {
		entry = entry.Str("task", *parsed.Task)
	}

	message := "Enrich policy execution succeeded"
	if isFailure {
		message = "Enrich policy execution failed"
	}
	entry.Msg(message)
	return !isFailure
}

// bulkInsert handles a batch of documents and validates per-item bulk response status.
func bulkInsert(es *elasticsearch.Client, index string, batch []map[string]interface{}, inserted, total int, idField string) bulkInsertResult {
	var buf strings.Builder
	for _, doc := range batch {
		meta := map[string]map[string]string{"index": {"_index": index}}

		if idField != "" {
			if v, ok := doc[idField]; ok {
				if idStr, ok := v.(string); ok && idStr != "" {
					meta["index"]["_id"] = idStr
				}
			}
		}

		metaLine, _ := json.Marshal(meta)
		docLine, _ := json.Marshal(doc)
		buf.Write(metaLine)
		buf.WriteByte('\n')
		buf.Write(docLine)
		buf.WriteByte('\n')
	}
	startTime := time.Now()
	res, err := es.Bulk(strings.NewReader(buf.String()), es.Bulk.WithContext(context.Background()))
	duration := time.Since(startTime)
	checkErr("bulk insert", err)
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		log.Fatal().
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Bulk API request failed")
	}

	var parsed bulkResponse
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		log.Fatal().Err(err).Msg("Unable to parse bulk response body")
	}

	failed := 0
	logged := 0
	for itemIdx, item := range parsed.Items {
		for action, result := range item {
			if result.Status >= 300 || result.Error != nil {
				failed++
				if logged < 10 {
					errorType := ""
					errorReason := ""
					if result.Error != nil {
						errorType = result.Error.Type
						errorReason = result.Error.Reason
					}
					log.Error().
						Int("item", itemIdx).
						Str("action", action).
						Str("_index", result.Index).
						Str("_id", result.ID).
						Int("status", result.Status).
						Str("error_type", errorType).
						Str("error_reason", errorReason).
						Msg("Bulk item failed")
					logged++
				}
			}
		}
	}

	if failed > 0 && failed > logged {
		log.Error().
			Int("failed_items", failed).
			Int("logged_failures", logged).
			Msg("Additional bulk item failures omitted from logs")
	}

	succeeded := len(batch) - failed
	log.Info().
		Int("inserted", inserted).
		Int("total", total).
		Int("batch_size", len(batch)).
		Int("succeeded", succeeded).
		Int("failed", failed).
		Float64("time_taken", duration.Seconds()).
		Msg("Batch processed")

	// TODO: Add targeted retries for retryable statuses (429/503) with exponential backoff.
	// TODO: Persist non-retryable item failures to a dead-letter file for later replay.
	return bulkInsertResult{Succeeded: succeeded, Failed: failed}
}
