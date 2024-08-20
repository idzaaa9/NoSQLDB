package utils

import "os"

func GetFileSize(file os.File) int {
	fileInfo, _ := file.Stat()
	return int(fileInfo.Size())
}
