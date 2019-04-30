//
//   Copyright © 2019 Uncharted Software Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package pipeline

import (
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"
)

func createResults(fittedSolutionID string, datasetURI string, resultPath string, targetFeature string, task TaskType) (string, error) {
	// load the source data
	dataPath := strings.Replace(datasetURI, "file://", "", 1)
	dataPath = strings.Replace(dataPath, "datasetDoc.json", "", 1)
	schema, err := loadDataSchema(dataPath)
	var resultDir string
	if err != nil {
		log.Warnf("generating unconstrained random data - %v", err)
		resultDir, err = generateDataNoSchema(fittedSolutionID, resultPath, targetFeature, task)
		if err != nil {
			return "", err
		}
	} else {
		resultDir, err = generateDataFromSchema(schema, fittedSolutionID, dataPath, resultPath, targetFeature, task)
		if err != nil {
			return "", err
		}
	}

	absResultDir, err := filepath.Abs(resultDir)
	if err != nil {
		return "", errors.Wrap(err, "Failed to generate absolute path")
	}

	return absResultDir, nil
}

func generateDataFromSchema(schema *DataSchema, fittedSolutionID string, dataPath string, resultPath string, targetFeature string, task TaskType) (string, error) {
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
	} else if task == TaskType_TIME_SERIES_FORECASTING {

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
		return "", errors.Errorf("unhandled task type %s", task)
	}

	// generate and persist mock result csv
	resultDir, err := generateResultCsv(fittedSolutionID, 0, dataPath, resultPath, d3mIndexCol, targetFeature, generator)
	if err != nil {
		return "", err
	}

	return resultDir, nil
}

func generateDataNoSchema(solutionID string, resultPath string, targetFeature string, task TaskType) (string, error) {
	// create stub data generators based on task
	var generator func(int) string
	if task == TaskType_CLASSIFICATION {
		categories := []string{"alpha", "bravo", "charlie"}
		numCategories := float64(len(categories))
		generator = func(index int) string {
			r := (rand.NormFloat64() + 1.0)
			f := math.Min(math.Max(0.0, r), numCategories-1)
			return categories[int(f)]
		}
	} else if task == TaskType_REGRESSION {
		generator = func(index int) string {
			return strconv.FormatFloat(rand.NormFloat64(), 'f', 4, 64)
		}
	} else if task == TaskType_TIME_SERIES_FORECASTING {
		generator = func(index int) string {
			return strconv.FormatFloat(rand.NormFloat64(), 'f', 4, 64)
		}
	} else {
		return "", errors.Errorf("unhandled task type %s", task)
	}

	// generate and persist mock result csv
	resultDir, err := generateResultCsv(solutionID, 0, "", resultPath, -1, targetFeature, generator)
	if err != nil {
		return "", err
	}

	return resultDir, nil
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
		if strings.EqualFold(fieldName, field) {
			fieldIndex = i
		}
	}

	if fieldIndex == -1 {
		return nil, fmt.Errorf("unable to find field index for field `%s`", fieldName)
	}

	// Map the index to the target value.
	lookup := make(map[string]string)
	for _, row := range data[1:] {
		if d3mIndexCol > len(row) {
			return nil, fmt.Errorf("`d3mIndexCol` index is outside range of row")
		}

		if fieldIndex > len(row) {
			return nil, fmt.Errorf("`fieldIndex` index is outside range of row")
		}
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
		if strings.EqualFold(fieldName, field) {
			fieldIndex = i
		}
	}

	if fieldIndex < 0 {
		return nil, errors.Errorf("Could not find field %s in data", fieldName)
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

	return keys, nil
}
