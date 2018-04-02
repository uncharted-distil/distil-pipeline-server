package pipeline

import (
	fmt "fmt"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"

	log "github.com/unchartedsoftware/plog"
)

func createResults(pipelineID string, datasetURI string, resultPath string, targetFeature string, task TaskType) (string, error) {
	// load the source data
	dataPath := strings.Replace(datasetURI, "file://", "", 1)
	dataPath = strings.Replace(dataPath, "datasetDoc.json", "", 1)
	schema, err := loadDataSchema(dataPath)
	if err != nil {
		log.Errorf("Error reading schema: %v", err)
		return "", err
	}

	d3mIndexCol := 0
	for i, v := range schema.DataResources[0].Variables {
		if v.ColName == "d3mIndex" {
			d3mIndexCol = i
		}
	}

	targetLookup, err := buildLookup(d3mIndexCol, dataPath, targetFeature)
	if err != nil {
		return "", err
	}

	// create stub data generators based on task
	var generator func(int) string
	if task == TaskType_CLASSIFICATION {
		cats, err := getCategories(dataPath, targetFeature)
		if err != nil {
			log.Errorf("Error generating data: %v", err)
			return "", err
		}

		generator = func(index int) string {
			if rand.Float32() > 0.9 {
				return cats[rand.Intn(len(cats))]
			}

			return targetLookup[fmt.Sprintf("%d", index)]
		}
	} else if task == TaskType_REGRESSION {
		generator = func(index int) string {
			var desiredMean float64
			targetValue := targetLookup[fmt.Sprintf("%d", index)]
			if targetValue != "" {
				desiredMean, err = strconv.ParseFloat(targetValue, 64)
				if err != nil {
					log.Errorf("Error generating data: %v", err)
					// TODO: use min & max values and randomly pick a value in between.
					return strconv.FormatFloat(rand.Float64(), 'f', 4, 64)
				}
			}

			adjustment := rand.Float64() * 0.1
			value := adjustment*desiredMean + desiredMean
			return strconv.FormatFloat(value, 'f', 4, 64)
		}
	} else {
		err := fmt.Errorf("unhandled task type %s", task)
		log.Error(err)
		return "", err
	}

	// generate and persist mock result csv
	resultDir, err := generateResultCsv(pipelineID, 0, dataPath, resultPath, d3mIndexCol, targetFeature, generator)
	if err != nil {
		log.Errorf("Failed to generate results: %s", err)
		return "", err
	}

	absResultDir, err := filepath.Abs(resultDir)
	if err != nil {
		log.Errorf("Failed to generate absolute path: %s", err)
		return "", err
	}

	return absResultDir, nil
}

func buildLookup(d3mIndexCol int, csvPath string, fieldName string) (map[string]string, error) {
	// Load the data
	data, err := loadDataCsv(csvPath)
	if err != nil {
		return nil, err
	}

	// Map the field name to an index.
	var fieldIndex = -1
	for i, field := range data[0] {
		if fieldName == field {
			fieldIndex = i
		}
	}

	// Map the index to the target value.
	lookup := make(map[string]string)
	for _, row := range data[1:] {
		lookup[row[d3mIndexCol]] = row[fieldIndex]
	}

	return lookup, nil
}

func getCategories(csvPath string, fieldName string) ([]string, error) {
	// Load the data
	data, err := loadDataCsv(csvPath)
	if err != nil {
		return nil, err
	}

	// Map the field name to an index.
	var fieldIndex = -1
	for i, field := range data[0] {
		if fieldName == field {
			fieldIndex = i
		}
	}

	log.Infof("%v", data[0])

	if fieldIndex < 0 {
		log.Errorf("Could not find field %s in data", fieldName)
		return nil, fmt.Errorf("Could not find field %s in data", fieldName)
	}

	// Get the distinct category values.
	categories := make(map[string]bool)
	for _, row := range data[1:] {
		if !categories[row[fieldIndex]] {
			categories[row[fieldIndex]] = true
		}
	}

	// Extract the keys to return the possible categories.
	i := 0
	keys := make([]string, len(categories))
	for k := range categories {
		keys[i] = k
		i++
	}
	log.Infof("Categories: %v", keys)

	return keys, nil
}
