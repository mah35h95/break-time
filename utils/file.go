package utils

import "os"

func WriteToFile(name string, data []byte) error {
	return os.WriteFile(name, data, 0644)
}
