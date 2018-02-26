package pipeline

import (
	"encoding/csv"
	"fmt"
	"os"
	"path"
)

func loadDataCsv(dirName string) ([][]string, error) {
	// load training data from the supplied directory
	f, err := os.Open(path.Join(dirName, "tables", "learningData.csv"))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, err
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
	pipelineID string,
	seqNum int,
	dirName string,
	resultDirName string,
	d3mIndexCol int,
	targetFeature string,
	resultGenerator func(int) string,
) (string, error) {

	// load training data - just use it to get count for now
	records, err := loadDataCsv(dirName)
	if err != nil {
		return "", err
	}

	// generate mock results skipping header row
	result := [][]string{{"d3mIndex", targetFeature}}
	for i := 1; i < len(records); i++ {
		result = append(result, []string{records[i][d3mIndexCol], resultGenerator(i)})
	}

	// write results out to disk
	path := path.Join(resultDirName, fmt.Sprintf("%s-%d", pipelineID, seqNum), "tables", "learningData.csv")
	return path, writeResultCsv(path, result)
}
