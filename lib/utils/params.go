package utils

import "math"

func CalculateMBF(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateKBF(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func CalculateMCMS(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

func CalculateKCMS(delta float64) uint {
	return uint(math.Ceil(math.Log(math.E / delta)))
}
