package migrator

import "log"

const (
	debugLevel    = "DEBUG"
	infoLevel     = "INFO"
	warningLevel  = "WARN"
	errorLevel    = "ERROR"
	criticalLevel = "CRIT"
)

type ILogger interface {
	IsDebugMode() bool
	Debug(msg string, ctx ...interface{})
	Info(msg string, ctx ...interface{})
	Warn(msg string, ctx ...interface{})
	Error(msg string, ctx ...interface{})
	Crit(msg string, ctx ...interface{})
}

type StdoutLogger struct {
	debugMode bool
}

func (s *StdoutLogger) IsDebugMode() bool {
	return s.debugMode
}

func (s *StdoutLogger) log(level, msg string, ctx ...interface{}) {
	r := append([]interface{}{level, msg}, ctx...)
	log.Println(r...)
}

func (s *StdoutLogger) Debug(msg string, ctx ...interface{}) {
	s.log(debugLevel, msg, ctx...)
}

func (s *StdoutLogger) Info(msg string, ctx ...interface{}) {
	s.log(infoLevel, msg, ctx...)
}

func (s *StdoutLogger) Warn(msg string, ctx ...interface{}) {
	s.log(warningLevel, msg, ctx...)
}

func (s *StdoutLogger) Error(msg string, ctx ...interface{}) {
	s.log(errorLevel, msg, ctx...)
}

func (s *StdoutLogger) Crit(msg string, ctx ...interface{}) {
	s.log(criticalLevel, msg, ctx...)
}

func NewStdoutLogger(debugMode bool) *StdoutLogger {
	return &StdoutLogger{debugMode: debugMode}
}
