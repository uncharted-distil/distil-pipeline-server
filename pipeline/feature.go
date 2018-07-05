package pipeline

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"
)

func createFeature(solutionID string, datasetURI string, resultURI string) (string, error) {
	dataPath := strings.Replace(datasetURI, "file://", "", 1)
	dataPath = strings.Replace(dataPath, "datasetDoc.json", "", 1)
	schema, err := loadDataSchema(dataPath)
	if err != nil {
		log.Warnf("unable to classify data since schema cannot be loaded - %v", err)
		return "", err
	}

	// find the d3m index column
	dataFile := ""
	indexColumn := -1
	for _, dr := range schema.DataResources {
		for _, v := range dr.Variables {
			if v.ColName == "d3mIndex" {
				dataFile = dr.ResPath
				indexColumn = v.ColIndex
			}
		}
	}
	if dataFile == "" {
		return "", errors.Errorf("no d3m index column found in dataset")
	}

	// initialize csv writer
	output := &bytes.Buffer{}
	writer := csv.NewWriter(output)
	err = writer.Write([]string{"d3mIndex", "labels", "probabilities"})
	if err != nil {
		return "", errors.Wrap(err, "unable to write header in output")
	}

	// read the data to output 1 row / index
	data, err := loadDataFileCsv(dataFile)
	if err != nil {
		return "", errors.Wrap(err, "unable to read data file")
	}

	for i := 0; i < len(data); i++ {
		err = writer.Write([]string{data[i][indexColumn], "cat,dog,mad_hat", "0.3,0.24,0.17"})
		if err != nil {
			return "", errors.Wrap(err, "unable to write header in output")
		}
	}

	resultDir := path.Join(resultURI, fmt.Sprintf("%s-%d", solutionID, 0))
	if err := os.MkdirAll(resultDir, 0777); err != nil && !os.IsExist(err) {
		return "", errors.Wrap(err, "unable to create summary output directory")
	}

	resultPath := path.Join(resultDir, "results.csv")

	writer.Flush()
	err = ioutil.WriteFile(resultPath, output.Bytes(), 0644)
	if err != nil {
		return "", errors.Wrap(err, "error writing summary output")
	}

	absResultPath, err := filepath.Abs(resultPath)
	if err != nil {
		return "", errors.Wrap(err, "Failed to generate absolute path")
	}

	return absResultPath, nil
}
