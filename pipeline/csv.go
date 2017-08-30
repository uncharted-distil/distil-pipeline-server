package pipeline

import (
	"encoding/csv"
	"os"
	"path"
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
	resultDir := path.Join(resultDirName, targetFeature)
	records, err := loadTrainCsv(trainDirName)
	if err != nil {
		return "", err
	}

	result := [][]string{{"d3m_index", targetFeature}}
	for i := 0; i < len(records); i++ {
		result = append(result, []string{string(i), resultGenerator()})
	}

	return resultDir, writeResultCsv(resultDir, result)
}
