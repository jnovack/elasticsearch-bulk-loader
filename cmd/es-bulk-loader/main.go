package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/jnovack/flag"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	version      = "dev"                  // default if not overridden
	buildRFC3339 = "1970-01-01T00:00:00Z" // default if not overridden
	revision     = "local"                // default if not overridden
)

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

type namedDefinitions map[string]json.RawMessage

type templateVariables map[string]string

type enrichFlagValue struct {
	enabled bool
	all     bool
	raw     string
}

type dataAction string

const (
	dataActionNone   dataAction = ""
	dataActionAdd    dataAction = "add"
	dataActionFlush  dataAction = "flush"
	dataActionDelete dataAction = "delete"
)

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
	pipelinesFile := flag.String("pipelines", "", "Path to JSON file containing one or more ingest pipeline definitions (optional)")
	policiesFile := flag.String("policies", "", "Path to JSON file containing one or more enrich policy definitions (optional)")
	dataFile := flag.String("data", "", "Path to bulk JSON data file (array of objects)")
	batchSize := flag.Int("batch", 1000, "Batch size for bulk inserts")
	deleteIndex := flag.Bool("delete", false, "Delete index if it exists")
	addToIndex := flag.Bool("add", false, "Add documents to existing index")
	flushIndex := flag.Bool("flush", false, "Delete all documents from an existing index without deleting the index")
	syncManaged := flag.Bool("sync-managed", false, "Create or update declared ingest pipelines and enrich policies")
	nuke := flag.Bool("nuke", false, "Delete the current index and declared managed resources, including dependent pipelines that reference declared enrich policies")
	idField := flag.String("id", "", "Field to use to override _id (not normal)")
	user := flag.String("user", "", "Username for basic auth (optional)")
	pass := flag.String("pass", "", "Password for basic auth (optional)")
	apiKey := flag.String("apiKey", "", "Elasticsearch API key (optional)")
	enrich := &enrichFlagValue{}
	flag.Var(enrich, "enrich", "Run enrich policies after the bulk insert; provide a comma-separated policy list or omit the value to run all policies")
	showVersion := flag.Bool("version", false, "print version and exit")

	flag.String(flag.DefaultConfigFlagname, "", "path to config file")

	flag.Parse()

	// Init logger
	zerolog.TimeFieldFormat = time.RFC3339
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	if *showVersion {
		log.Info().
			Str("version", version).
			Str("build_rfc3339", buildRFC3339).
			Str("revision", revision).
			Msg("jnovack/es-bulk-loader")
		os.Exit(0)
	}

	log.Info().
		Str("version", version).
		Str("build_rfc3339", buildRFC3339).
		Str("revision", revision).
		Msg("jnovack/es-bulk-loader starting...")

	if (*user != "" || *pass != "") && *apiKey != "" {
		log.Info().Msg("Cannot use both basic auth and API key. Choose one method.")
		flag.Usage()
		os.Exit(1)
	}

	action, err := selectedDataAction(*addToIndex, *flushIndex, *deleteIndex)
	if err != nil {
		log.Info().Err(err).Msg("Invalid flag combination")
		flag.Usage()
		os.Exit(1)
	}

	if *index == "" {
		flag.Usage()
		os.Exit(1)
	}

	if action.requiresDataFile() && *dataFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	if action == dataActionNone && !*syncManaged && !*nuke && !enrich.enabled {
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

	variables := buildTemplateVariables(*index)
	pipelineDefinitions, pipelineNames := readNamedDefinitions(*pipelinesFile, "pipeline", variables)
	policyDefinitions, policyNames := readNamedDefinitions(*policiesFile, "policy", variables)
	defaultPipeline := ""
	if *syncManaged && len(pipelineNames) > 0 {
		defaultPipeline = pipelineNames[0]
	}

	// Determine if index exists
	exists, err := indexExists(es, *index)
	checkErr("checking if index exists", err)

	if *nuke {
		if exists {
			log.Warn().Str("index", *index).Msg("Nuke deleting index and declared managed resources")
			deleteAndCheck(es, *index)
			exists = false
		} else {
			log.Warn().Str("index", *index).Msg("Index does not exist. Nuke will still remove declared managed resources")
		}

		deleteManagedResources(es, pipelineNames, policyNames, true)
	}

	switch action {
	case dataActionDelete:
		if exists {
			log.Info().Str("index", *index).Msg("Deleting index before reloading data")
			deleteAndCheck(es, *index)
			exists = false
		} else {
			log.Warn().Str("index", *index).Msg("Index does not exist. Nothing to delete.")
		}

		deleteManagedResources(es, pipelineNames, policyNames, false)
	case dataActionFlush:
		if exists {
			log.Info().Str("index", *index).Msg("Flushing existing index before loading replacement data")
			flushAndCheck(es, *index)
		} else {
			log.Warn().Str("index", *index).Msg("Index does not exist. Nothing to flush.")
		}
	case dataActionAdd:
		if exists {
			log.Info().Str("index", *index).Msg("Appending documents to existing index")
		} else {
			log.Info().Str("index", *index).Msg("Creating index to append documents")
		}
	}

	shouldCreateIndex := action.requiresDataFile() && !exists

	if shouldCreateIndex && *syncManaged {
		createPipelines(es, pipelineDefinitions, pipelineNames)
	}

	// Create index if needed
	if shouldCreateIndex {
		body := buildCreateIndexBody(*settingsFile, *mappingsFile, defaultPipeline, variables)
		res, err := es.Indices.Create(*index, es.Indices.Create.WithBody(strings.NewReader(body)))
		checkErr("creating index", err)
		defer res.Body.Close()
		if res.IsError() {
			responseBody, _ := io.ReadAll(res.Body)
			log.Fatal().
				Str("index", *index).
				Int("status_code", res.StatusCode).
				Str("body", string(responseBody)).
				Msg("Failed to create index")
		}
		waitForIndex(es, *index)
		exists = true
		log.Info().Str("index", *index).Msg("Index created")
	}

	if *syncManaged && exists {
		if !shouldCreateIndex {
			createPipelines(es, pipelineDefinitions, pipelineNames)
		}
		createPolicies(es, policyDefinitions, policyNames)
	}

	if action.requiresDataFile() {
		log.Info().Msg("Starting bulk insert")

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

		if _, err := f.Seek(0, 0); err != nil {
			log.Fatal().Err(err).Msg("Error rewinding data file")
		}
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
		}
	}

	if enrich.enabled {
		refreshIndex(es, *index)
		runEnrichPolicies(es, enrich, policyNames)
	}

}

func checkErr(context string, err error) {
	log.Trace().Msg(context)
	if err != nil {
		log.Fatal().Err(err).Msgf("Error during %s", context)
	}
}

func (a dataAction) requiresDataFile() bool {
	return a != dataActionNone
}

func selectedDataAction(addToIndex, flushIndex, deleteIndex bool) (dataAction, error) {
	actionCount := 0
	if addToIndex {
		actionCount++
	}
	if flushIndex {
		actionCount++
	}
	if deleteIndex {
		actionCount++
	}
	if actionCount > 1 {
		return dataActionNone, fmt.Errorf("-add, -flush, and -delete are mutually exclusive")
	}

	switch {
	case addToIndex:
		return dataActionAdd, nil
	case flushIndex:
		return dataActionFlush, nil
	case deleteIndex:
		return dataActionDelete, nil
	default:
		return dataActionNone, nil
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

func waitForIndex(es *elasticsearch.Client, index string) {
	for i := 0; i < 20; i++ {
		exists, err := indexExists(es, index)
		checkErr("waiting for index creation", err)
		if exists {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}

	log.Fatal().Str("index", index).Msg("Index create was acknowledged but the index did not become visible")
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

func buildCreateIndexBody(settingsFile, mappingsFile, defaultPipeline string, variables templateVariables) string {
	settings := normalizeIndexSettings(settingsFile, defaultPipeline, variables)
	mappings := normalizeIndexSection(mappingsFile, "mappings", variables)

	return fmt.Sprintf(`{"settings": %s, "mappings": %s}`, settings, mappings)
}

func normalizeIndexSettings(path, defaultPipeline string, variables templateVariables) string {
	settings := make(map[string]json.RawMessage)
	if path != "" {
		content, err := readTemplatedFile(path, variables)
		if err != nil {
			log.Fatal().Err(err).Str("path", path).Msg("Reading settings file")
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(content, &raw); err != nil {
			log.Fatal().Err(err).Str("path", path).Msg("Parsing settings file")
		}

		source := raw
		if nested, ok := raw["settings"]; ok {
			source = nil
			if err := json.Unmarshal(nested, &source); err != nil {
				log.Fatal().Err(err).Str("path", path).Msg("Parsing nested settings file")
			}
		}

		for key, value := range source {
			if key == "index" {
				var nested map[string]json.RawMessage
				if err := json.Unmarshal(value, &nested); err == nil {
					for nestedKey, nestedValue := range nested {
						normalizedKey := normalizeSettingKey(nestedKey)
						if _, exists := settings[normalizedKey]; exists {
							continue
						}
						settings[normalizedKey] = nestedValue
					}
					continue
				}
			}

			settings[normalizeSettingKey(key)] = value
		}
	}

	if defaultPipeline != "" {
		if _, ok := settings["default_pipeline"]; !ok {
			settings["default_pipeline"] = json.RawMessage(strconv.Quote(defaultPipeline))
			log.Info().Str("pipeline", defaultPipeline).Msg("Using first declared pipeline as index.default_pipeline")
		}
	}

	normalized, err := json.Marshal(settings)
	if err != nil {
		log.Fatal().Err(err).Str("path", path).Msg("Serializing settings")
	}

	return string(normalized)
}

func normalizeSettingKey(key string) string {
	return strings.TrimPrefix(key, "index.")
}

func normalizeIndexSection(path, section string, variables templateVariables) string {
	if path == "" {
		return "{}"
	}

	content, err := readTemplatedFile(path, variables)
	if err != nil {
		log.Fatal().Err(err).Str("path", path).Msgf("Reading %s file", section)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(content, &raw); err != nil {
		log.Fatal().Err(err).Str("path", path).Msgf("Parsing %s file", section)
	}

	if nested, ok := raw[section]; ok {
		return string(nested)
	}

	return string(content)
}

func readNamedDefinitions(path, resourceType string, variables templateVariables) (namedDefinitions, []string) {
	if path == "" {
		return nil, nil
	}

	content, err := readTemplatedFile(path, variables)
	if err != nil {
		log.Fatal().Err(err).Str("path", path).Msgf("Reading %s definitions file", resourceType)
	}

	decoder := json.NewDecoder(strings.NewReader(string(content)))
	token, err := decoder.Token()
	if err != nil {
		log.Fatal().Err(err).Str("path", path).Msgf("Parsing %s definitions file", resourceType)
	}

	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		log.Fatal().Str("path", path).Msgf("%s definitions file must contain a JSON object", resourceType)
	}

	definitions := make(namedDefinitions)
	names := make([]string, 0)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			log.Fatal().Err(err).Str("path", path).Msgf("Reading %s definition name", resourceType)
		}

		name, ok := keyToken.(string)
		if !ok {
			log.Fatal().Str("path", path).Msgf("Invalid %s definition name", resourceType)
		}

		var definition json.RawMessage
		if err := decoder.Decode(&definition); err != nil {
			log.Fatal().Err(err).Str("path", path).Msgf("Parsing %s definition body", resourceType)
		}

		definitions[name] = definition
		names = append(names, name)
	}

	if _, err := decoder.Token(); err != nil {
		log.Fatal().Err(err).Str("path", path).Msgf("Parsing %s definitions file", resourceType)
	}

	return definitions, names
}

func buildTemplateVariables(index string) templateVariables {
	return templateVariables{
		"INDEX": index,
	}
}

var templateVariablePattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)`)

func readTemplatedFile(path string, variables templateVariables) ([]byte, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	expanded := templateVariablePattern.ReplaceAllStringFunc(string(content), func(match string) string {
		name := strings.TrimPrefix(match, "$")
		name = strings.TrimPrefix(name, "{")
		name = strings.TrimSuffix(name, "}")

		if value, ok := variables[name]; ok {
			return value
		}
		if value, ok := os.LookupEnv(name); ok {
			return value
		}
		return match
	})

	return []byte(expanded), nil
}

func createPipelines(es *elasticsearch.Client, definitions namedDefinitions, names []string) {
	for _, name := range names {
		res, err := es.Ingest.PutPipeline(
			name,
			strings.NewReader(string(definitions[name])),
			es.Ingest.PutPipeline.WithContext(context.Background()),
		)
		checkErr("creating pipeline", err)

		if res.IsError() {
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			log.Fatal().
				Str("pipeline", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to create pipeline")
		}
		res.Body.Close()

		log.Info().Str("pipeline", name).Msg("Pipeline created or updated")
	}
}

func deletePipelines(es *elasticsearch.Client, names []string) {
	for _, name := range names {
		res, err := es.Ingest.DeletePipeline(
			name,
			es.Ingest.DeletePipeline.WithContext(context.Background()),
		)
		checkErr("deleting pipeline", err)

		if res.StatusCode == http.StatusNotFound {
			res.Body.Close()
			log.Info().Str("pipeline", name).Msg("Pipeline does not exist. Nothing to delete.")
			continue
		}
		if res.IsError() {
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			log.Fatal().
				Str("pipeline", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to delete pipeline")
		}
		res.Body.Close()

		log.Info().Str("pipeline", name).Msg("Pipeline deleted")
	}
}

func createPolicies(es *elasticsearch.Client, definitions namedDefinitions, names []string) {
	for _, name := range names {
		for attempt := 1; attempt <= 5; attempt++ {
			res, err := es.EnrichPutPolicy(
				name,
				strings.NewReader(string(definitions[name])),
				es.EnrichPutPolicy.WithContext(context.Background()),
				es.EnrichPutPolicy.WithHeader(map[string]string{
					"Content-Type": "application/json",
					"Accept":       "application/json",
				}),
			)
			checkErr("creating enrich policy", err)

			if res.IsError() {
				body, _ := io.ReadAll(res.Body)
				res.Body.Close()
				if isUnsupportedEnrichAPI(res.StatusCode, body) {
					log.Warn().
						Int("status_code", res.StatusCode).
						Str("body", string(body)).
						Msg("Enrich policy endpoint returned a generic 404; check proxy or routing for /_enrich/policy and confirm this URL matches the backend used by Dev Tools")
					return
				}
				if hasElasticsearchErrorType(body, "index_not_found_exception") && attempt < 5 {
					log.Warn().
						Str("policy", name).
						Int("attempt", attempt).
						Int("status_code", res.StatusCode).
						Str("body", string(body)).
						Msg("Source index for enrich policy is not visible yet; retrying enrich policy creation")
					time.Sleep(500 * time.Millisecond)
					continue
				}
				if hasElasticsearchErrorType(body, "resource_already_exists_exception") {
					log.Info().
						Str("policy", name).
						Msg("Enrich policy already exists. Leaving it in place")
					break
				}
				log.Fatal().
					Str("policy", name).
					Int("status_code", res.StatusCode).
					Str("body", string(body)).
					Msg("Failed to create enrich policy")
			}
			res.Body.Close()

			log.Info().Str("policy", name).Msg("Created enrich policy")
			break
		}
	}
}

func deletePolicies(es *elasticsearch.Client, names []string, nuke bool) {
	for _, name := range names {
		for attempt := 1; attempt <= 2; attempt++ {
			res, err := es.EnrichDeletePolicy(
				name,
				es.EnrichDeletePolicy.WithContext(context.Background()),
				es.EnrichDeletePolicy.WithHeader(map[string]string{
					"Accept": "application/json",
				}),
			)
			checkErr("deleting enrich policy", err)

			if res.StatusCode == http.StatusNotFound {
				body, _ := io.ReadAll(res.Body)
				res.Body.Close()
				if isUnsupportedEnrichAPI(res.StatusCode, body) {
					log.Warn().
						Int("status_code", res.StatusCode).
						Str("body", string(body)).
						Msg("Enrich policy endpoint returned a generic 404; check proxy or routing for /_enrich/policy and confirm this URL matches the backend used by Dev Tools")
					return
				}
				log.Debug().Str("policy", name).Msg("Enrich policy does not exist. Nothing to delete.")
				break
			}
			if res.IsError() {
				body, _ := io.ReadAll(res.Body)
				res.Body.Close()
				if res.StatusCode == http.StatusConflict && nuke && policyDeleteBlockedByPipelineReference(body) && attempt == 1 {
					referencing := findPipelinesReferencingPolicy(es, name)
					if len(referencing) == 0 {
						log.Fatal().
							Str("policy", name).
							Int("status_code", res.StatusCode).
							Str("body", string(body)).
							Msg("Failed to delete enrich policy; nuke mode could not find referencing pipelines")
					}

					log.Warn().
						Str("policy", name).
						Strs("pipelines", referencing).
						Msg("Nuke mode deleting pipelines that reference this enrich policy before retrying policy deletion")
					deletePipelines(es, referencing)
					continue
				}

				log.Fatal().
					Str("policy", name).
					Int("status_code", res.StatusCode).
					Str("body", string(body)).
					Msg("Failed to delete enrich policy")
			}
			res.Body.Close()

			log.Info().Str("policy", name).Msg("Deleted enrich policy")
			break
		}
	}
}

func deleteManagedResources(es *elasticsearch.Client, pipelineNames []string, policyNames []string, nuke bool) {
	if len(pipelineNames) > 0 {
		deletePipelines(es, pipelineNames)
	}
	if len(policyNames) > 0 {
		deletePolicies(es, policyNames, nuke)
	}
}

func findPipelinesReferencingPolicy(es *elasticsearch.Client, policy string) []string {
	res, err := es.Ingest.GetPipeline(
		es.Ingest.GetPipeline.WithContext(context.Background()),
	)
	checkErr("getting ingest pipelines", err)
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return nil
	}
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		log.Fatal().
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Failed to get ingest pipelines")
	}

	var definitions namedDefinitions
	if err := json.NewDecoder(res.Body).Decode(&definitions); err != nil {
		log.Fatal().Err(err).Msg("Unable to parse ingest pipeline response")
	}

	return pipelineNamesReferencingPolicy(definitions, policy)
}

func pipelineNamesReferencingPolicy(definitions namedDefinitions, policy string) []string {
	names := make([]string, 0)
	for name, definition := range definitions {
		if pipelineDefinitionReferencesPolicy(definition, policy) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

func pipelineDefinitionReferencesPolicy(definition json.RawMessage, policy string) bool {
	var parsed any
	if err := json.Unmarshal(definition, &parsed); err != nil {
		return false
	}
	return valueReferencesPolicy(parsed, policy)
}

func valueReferencesPolicy(value any, policy string) bool {
	switch typed := value.(type) {
	case map[string]any:
		if enrich, ok := typed["enrich"].(map[string]any); ok {
			if policyName, ok := enrich["policy_name"].(string); ok && policyName == policy {
				return true
			}
		}
		for _, nested := range typed {
			if valueReferencesPolicy(nested, policy) {
				return true
			}
		}
	case []any:
		for _, nested := range typed {
			if valueReferencesPolicy(nested, policy) {
				return true
			}
		}
	}
	return false
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

	log.Info().Str("index", index).Msg("Refreshed index before executing enrich policy")
}

func runEnrichPolicies(es *elasticsearch.Client, enrich *enrichFlagValue, declared []string) {
	availablePolicies, supported := getEnrichPolicies(es)
	if !supported {
		return
	}
	if len(availablePolicies) == 0 {
		log.Warn().Msg("No enrich policies found; skipping enrich execution")
		return
	}

	targets, missing := resolveEnrichTargets(enrich, availablePolicies, declared)
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
		Msg("Completed enrich policy execution")
}

func getEnrichPolicies(es *elasticsearch.Client) ([]string, bool) {
	res, err := es.EnrichGetPolicy(
		es.EnrichGetPolicy.WithContext(context.Background()),
		es.EnrichGetPolicy.WithHeader(map[string]string{
			"Accept": "application/json",
		}),
	)
	checkErr("getting enrich policies", err)
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		if isUnsupportedEnrichAPI(res.StatusCode, body) {
			log.Warn().
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Enrich policy endpoint returned a generic 404; check proxy or routing for /_enrich/policy and confirm this URL matches the backend used by Dev Tools")
			return nil, false
		}
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
	return policies, true
}

func resolveEnrichTargets(enrich *enrichFlagValue, available []string, declared []string) ([]string, []string) {
	if enrich == nil || !enrich.enabled {
		return nil, nil
	}

	availableSet := make(map[string]struct{}, len(available))
	for _, policy := range available {
		availableSet[policy] = struct{}{}
	}

	if enrich.all {
		if len(declared) > 0 {
			targets := make([]string, 0, len(declared))
			missing := make([]string, 0)
			for _, policy := range declared {
				if _, ok := availableSet[policy]; ok {
					targets = append(targets, policy)
					continue
				}
				missing = append(missing, policy)
			}
			return targets, missing
		}

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
		es.EnrichExecutePolicy.WithHeader(map[string]string{
			"Accept": "application/json",
		}),
	)
	if err != nil {
		log.Error().Err(err).Str("policy", policy).Msg("Failed to execute enrich policy")
		return false
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		if isUnsupportedEnrichAPI(res.StatusCode, body) {
			log.Warn().
				Str("policy", policy).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Enrich execute endpoint returned a generic 404; check proxy or routing for /_enrich/policy/<name>/_execute and confirm this URL matches the backend used by Dev Tools")
			return false
		}
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
	event := log.Debug()
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

func isUnsupportedEnrichAPI(statusCode int, body []byte) bool {
	if statusCode != http.StatusNotFound {
		return false
	}

	if bytes.Contains(body, []byte(`"error":{`)) {
		return false
	}

	return !bytes.Contains(body, []byte("resource_not_found_exception"))
}

func hasElasticsearchErrorType(body []byte, errorType string) bool {
	return bytes.Contains(body, []byte(`"type":"`+errorType+`"`))
}

func policyDeleteBlockedByPipelineReference(body []byte) bool {
	return bytes.Contains(body, []byte("pipeline is referencing it"))
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
	log.Debug().
		Int("inserted", inserted).
		Int("total", total).
		Int("batch_size", len(batch)).
		Int("succeeded", succeeded).
		Int("failed", failed).
		Float64("time_taken", duration.Seconds()).
		Msg("Processed batch")

	// TODO: Add targeted retries for retryable statuses (429/503) with exponential backoff.
	// TODO: Persist non-retryable item failures to a dead-letter file for later replay.
	return bulkInsertResult{Succeeded: succeeded, Failed: failed}
}
