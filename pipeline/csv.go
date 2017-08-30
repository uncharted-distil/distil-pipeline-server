package pipeline

import (
	"encoding/csv"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
)

func loadTrainCsv(dirName string) ([][]string, error) {
	f, err := os.Open(path.Join(dirName, "trainData.csv"))
	if err != nil {
		return nil, err
	}
	defer f.Close() // this needs to be after the err check

	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, err
	}
	return lines, nil
}

func writeResultCsv(path string, data [][]string) error {
	// create a result directory
	err := os.MkdirAll(filepath.Dir(path), 0700)
	if err != nil {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

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

func generateResultCsv(trainDirName string, resultDirName string, targetFeature string, resultGenerator func() string) (string, error) {
	// load training data so we can get the number records
	records, err := loadTrainCsv(trainDirName)
	if err != nil {
		return "", err
	}

	// generate results
	result := [][]string{{"d3m_index", targetFeature}}
	for i := 0; i < len(records); i++ {
		result = append(result, []string{strconv.Itoa(i), resultGenerator()})
	}

	// write results out to disk
	path := path.Join(resultDirName, fmt.Sprintf("%s.csv", targetFeature))
	return path, writeResultCsv(path, result)
}
