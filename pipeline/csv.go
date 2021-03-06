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
	"encoding/csv"
	"fmt"
	"os"
	"path"
	"strconv"

	"github.com/pkg/errors"
)

const defaultRecordCount = 100

func loadDataCsv(dirName string) ([][]string, error) {
	// load training data from the supplied directory
	return loadDataFileCsv(path.Join(dirName, "tables", "learningData.csv"))
}

func loadDataFileCsv(filename string) ([][]string, error) {
	// load training data from the supplied directory
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load csv from `%s`", filename)
	}
	return lines, nil
}

func writeResultCsv(resultPath string, data [][]string) error {
	// create result directory if necessary
	err := os.MkdirAll(path.Dir(resultPath), 0777)
	if err != nil {
		return err
	}

	// create the result file
	file, err := os.Create(resultPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// write teh result into it and close
	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, value := range data {
		err := writer.Write(value)
		if err != nil {
			return err
		}
	}

	return nil
}

func generateResultCsv(
	solutionID string,
	seqNum int,
	dirName string,
	resultDirName string,
	d3mIndexCol int,
	targetFeature string,
	resultGenerator func(int) string,
) (string, error) {
	recordCount := defaultRecordCount
	var records [][]string
	var err error

	if dirName != "" && d3mIndexCol >= 0 {
		// load training data - just use it to get count for now
		records, err = loadDataCsv(dirName)
		if err != nil {
			return "", err
		}
		recordCount = len(records)
	}

	// generate mock results skipping header row - use d3m index from training data if its available
	result := [][]string{{"d3mIndex", targetFeature}}
	for i := 1; i < recordCount; i++ {
		var d3mIndex int
		if records != nil {
			d3mIndex, err = strconv.Atoi(records[i][d3mIndexCol])
		} else {
			d3mIndex = i
		}
		result = append(result, []string{strconv.Itoa(d3mIndex), resultGenerator(d3mIndex)})
	}

	// write results out to disk
	path := path.Join(resultDirName, fmt.Sprintf("%s-%d", solutionID, seqNum), "tables", "learningData.csv")
	return path, writeResultCsv(path, result)
}
