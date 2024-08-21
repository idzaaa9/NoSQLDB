package utils

import "os"

func GetFileSize(file os.File) int {
	fileInfo, _ := file.Stat()
	return int(fileInfo.Size())
}

func IsEmptyDir(path string) bool {
	dir, _ := os.Open(path)
	defer dir.Close()
	_, err := dir.ReadDir(1)
	if err != nil {
		return true
	}
	return false
}
