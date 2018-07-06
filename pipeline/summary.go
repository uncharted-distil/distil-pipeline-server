package pipeline

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/pkg/errors"
)

func createSummary(solutionID string, datasetURI string, resultURI string) (string, error) {
	// initialize csv writer
	output := &bytes.Buffer{}
	writer := csv.NewWriter(output)

	// output should be header row, and 1 row per token (token,probability)
	err := writer.Write([]string{"token", "probability"})
	if err != nil {
		return "", errors.Wrap(err, "unable to write header in output")
	}
	err = writer.Write([]string{"fake_label_1", "0.75"})
	if err != nil {
		return "", errors.Wrap(err, "unable to write data in output")
	}
	err = writer.Write([]string{"fake_label_2", "0.55"})
	if err != nil {
		return "", errors.Wrap(err, "unable to write data in output")
	}
	err = writer.Write([]string{"fake_label_3", "0.35"})
	if err != nil {
		return "", errors.Wrap(err, "unable to write data in output")
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
