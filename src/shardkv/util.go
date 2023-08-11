package shardkv

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

const limit = 0

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	DClient  logTopic = "CLNT"
	DServer  logTopic = "SERV"
	DCommit  logTopic = "CMIT"
	DDrop    logTopic = "DROP"
	DError   logTopic = "ERRO"
	DInfo    logTopic = "INFO"
	DLeader  logTopic = "LEAD"
	DApply   logTopic = "APPL"
	DLog     logTopic = "LOG1"
	DLog2    logTopic = "LOG2"
	DPersist logTopic = "PERS"
	DSnap    logTopic = "SNAP"
	DTerm    logTopic = "TERM"
	DTest    logTopic = "TEST"
	DTimer   logTopic = "TIMR"
	DTrace   logTopic = "TRCE"
	DVote    logTopic = "VOTE"
	DWarn    logTopic = "WARN"
	DForC    logTopic = "FORC"
	DHeart   logTopic = "HEAR"
	DState   logTopic = "STAT"
	DTemp    logTopic = "TEMP"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DPrintf(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity > limit {
		duration := time.Since(debugStart).Microseconds()
		duration /= 100
		prefix := fmt.Sprintf("%06d %v ", duration, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func DeepCopy(sth interface{}) interface{} {
	switch t := sth.(type) {
	default:
		fmt.Printf("unexpected type %v\n", t) // %T prints whatever type t has
	case map[int][]string:
		res := make(map[int][]string)
		for k, v := range t {
			res[k] = v
		}
		return res
	case map[int64]int:
		res := make(map[int64]int)
		for k, v := range t {
			res[k] = v
		}
		return res
	case []string:
		res := make([]string, len(t))
		copy(res, t)
		return res
	case *Shard:
		lastCommand := make(map[int64]int)
		logs := make(map[string]string)
		for k, v := range t.Log {
			logs[k] = v
		}
		for k, v := range t.LastCommand {
			lastCommand[k] = v
		}
		res := Shard{Log: logs, LastCommand: lastCommand, Status: t.Status}
		return &res
	}
	return ""
}
