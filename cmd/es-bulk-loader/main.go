package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/jnovack/flag"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var version = "dev" // default if not overridden

type bulkResponse struct {
	Errors bool                           `json:"errors"`
	Items  []map[string]bulkItemResponse  `json:"items"`
}

type bulkItemResponse struct {
	Index  string          `json:"_index"`
	ID     string          `json:"_id"`
	Status int             `json:"status"`
	Error  *bulkItemError  `json:"error,omitempty"`
}

type bulkItemError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

type bulkInsertResult struct {
	Succeeded int
	Failed    int
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
