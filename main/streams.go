package main

import (
	"fmt"
	"strconv"
	"strings"
)

type StreamKV struct {
	key   string
	value string
}

type StreamEntry struct {
	Id     string
	Values []StreamKV
}

func validateStreamId(prevId string, id string) error {
	prevSplitId := strings.Split(prevId, "-")
	splitId := strings.Split(id, "-")
	if len(splitId) != 2 {
		return fmt.Errorf("ERR The ID specified in XADD need to be in <millisecondtime>-<sequencenumber> format.")
	}
	millisecondTime, err := strconv.Atoi(splitId[0])
	if err != nil {
		return fmt.Errorf("ERR The ID specified in XADD need to have valid number as <millisecondtime>")
	}
	sequenceNumber, err := strconv.Atoi(splitId[1])
	if err != nil {
		return fmt.Errorf("ERR The ID specified in XADD need to have valid number as <sequencenumber>")
	}
	prevMillisecondTime, _ := strconv.Atoi(prevSplitId[0])
	prevSequenceNumber, _ := strconv.Atoi(prevSplitId[1])
	if millisecondTime < prevMillisecondTime {
		return fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
	if millisecondTime == prevMillisecondTime && prevSequenceNumber >= sequenceNumber {
		return fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
	return nil
}
