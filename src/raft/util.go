package raft

import "log"
import "time"
import "math/rand"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Should go to some util funcs.
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// Should go to some util funcs.
func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func assert(expr bool) {
	if !expr {
		panic("Programming error")
	}
}

// Will return a value that majority of elements are greater than.
// e.g [6, 7, 8] -> 7
//     [1, 1, 1, 2, 3] -> 1
// Assume array size > 0
func getMajorityMax(array []int) int {
	// Find min and increase monotonically, hit and break.
	min := array[0]
	for _, ele := range array {
		if ele < min {
			min = ele
		}
	}
	for {
		c := 0
		for i := 0; i < len(array); i++ {
			if array[i] >= min {
				c++
			}
		}
		if c > len(array)/2 {
			min++
		} else {
			break
		}
	}
	return min - 1
}

func getElectionTimeout() time.Duration {
	return time.Duration(rand.Int63n(300)+250) * time.Millisecond
}
