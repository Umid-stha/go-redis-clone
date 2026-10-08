package main

import (
	"fmt"
	"strconv"
)

type StreamKV struct {
	key   string
	value string
}

type StreamEntry struct {
	Id     string
	Values []StreamKV
}

func validateStreamId(prevMillisecondTime int, prevSequenceNumber int, millisecond string, sequence string) error {
	millisecondTime, err := strconv.Atoi(millisecond)
	if err != nil {
		return fmt.Errorf("ERR The ID specified in XADD need to have valid number as <millisecondtime>")
	}
	sequenceNumber, err := strconv.Atoi(sequence)
	if err != nil {
		return fmt.Errorf("ERR The ID specified in XADD need to have valid number as <sequencenumber>")
	}
	if millisecondTime < prevMillisecondTime {
		return fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
	if millisecondTime == prevMillisecondTime && prevSequenceNumber >= sequenceNumber {
		return fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
	return nil
}

func autoGenerateSequenceId(prevSequenceNumber int) string {
	return strconv.Itoa(prevSequenceNumber + 1)
}
