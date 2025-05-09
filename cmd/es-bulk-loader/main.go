package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jnovack/flag"

	elasticsearch "github.com/elastic/go-elasticsearch/v9"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

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
	addToIndex := flag.Bool("add", false, "Add documents to existing index without modifying it")
	user := flag.String("user", "", "Username for basic auth (optional)")
	pass := flag.String("pass", "", "Password for basic auth (optional)")
	apiKey := flag.String("apiKey", "", "Elasticsearch API key (optional)")

	flag.String(flag.DefaultConfigFlagname, "", "path to config file")

	flag.Parse()

	// Init logger
	zerolog.TimeFieldFormat = time.RFC3339
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

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
		case *addToIndex && *deleteIndex:
			log.Info().Str("index", *index).Msg("Deleting and recreating index before adding documents")
			deleteAndCheck(es, *index)
			exists = false
		case *deleteIndex:
			log.Info().Str("index", *index).Msg("Deleting index")
			deleteAndCheck(es, *index)
			exists = false
		case *addToIndex:
			log.Info().Str("index", *index).Msg("Appending documents to existing index")
		default:
			log.Fatal().
				Str("index", *index).
				Msg("Index exists. Use -delete to recreate or -add to append.")
		}
	} else {
		if *deleteIndex {
			log.Warn().Str("index", *index).Msg("Index does not exist. Nothing to delete.")
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

	// Load data
	data, err := os.ReadFile(*dataFile)
	checkErr("reading data file", err)

	var records []map[string]interface{}
	err = json.Unmarshal(data, &records)
	checkErr("parsing data JSON", err)

	total := len(records)
	log.Info().Int("total", total).Msg("Starting bulk insert")

	overallStart := time.Now()
	for i := 0; i < total; i += *batchSize {
		end := i + *batchSize
		if end > total {
			end = total
		}

		var buf bytes.Buffer
		for _, doc := range records[i:end] {
			meta := map[string]map[string]string{"index": {"_index": *index}}
			metaLine, _ := json.Marshal(meta)
			docLine, _ := json.Marshal(doc)

			buf.Write(metaLine)
			buf.WriteByte('\n')
			buf.Write(docLine)
			buf.WriteByte('\n')
		}

		startTime := time.Now()
		res, err := es.Bulk(bytes.NewReader(buf.Bytes()), es.Bulk.WithContext(context.Background()))
		duration := time.Since(startTime)

		checkErr("bulk insert", err)
		res.Body.Close()

		log.Info().
			Int("inserted", end).
			Int("total", total).
			Float64("batch_time_s", duration.Seconds()).
			Msg("Batch inserted")
	}

	overallDuration := time.Since(overallStart)
	log.Info().Float64("total_time_s", overallDuration.Seconds()).Msg("Bulk load completed")
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

	if res.StatusCode == 200 {
		return true, nil
	} else if res.StatusCode == 404 {
		return false, nil
	}
	return false, fmt.Errorf("unexpected status code %d", res.StatusCode)
}

func deleteAndCheck(es *elasticsearch.Client, index string) {
	res, err := es.Indices.Delete([]string{index})
	checkErr("deleting index", err)
	defer res.Body.Close()

	if res.IsError() {
		log.Fatal().Str("index", index).Msg("Failed to delete index")
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
