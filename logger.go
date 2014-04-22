package xorm

import (
	"io"
	"log"
)

// logger interface, log/syslog compatible with this interface
type LogLevel int

const (

	// From /usr/include/sys/syslog.h.
	// These are the same on Linux, BSD, and OS X.
	LOG_OFF LogLevel = -1
	LOG_ERR          = 3
	LOG_WARNING
	LOG_INFO = 6
	LOG_DEBUG
)

type ILogger interface {
	Err(m string) (err error)
	Warning(m string) (err error)
	Info(m string) (err error)
	Debug(m string) (err error)
}

type SimpleLogger struct {
	logger   *log.Logger
	LogLevel LogLevel
}

func NewSimpleLogger(out io.Writer) *SimpleLogger {
	return &SimpleLogger{
		logger: log.New(out, "[xorm] ", log.Ldate|log.Lmicroseconds), LogLevel: LOG_INFO}
}

func NewSimpleLogger2(out io.Writer, prefix string, flag int) *SimpleLogger {
	return &SimpleLogger{
		logger: log.New(out, prefix, flag), LogLevel: LOG_INFO}
}

func NewSimpleLoggerWithLevel(out io.Writer, logLevel LogLevel) *SimpleLogger {
	return &SimpleLogger{
		logger: log.New(out, "[xorm] ", log.Ldate|log.Lmicroseconds), LogLevel: logLevel}
}

func NewSimpleLogger2WithLevel(out io.Writer, prefix string, flag int, logLevel LogLevel) *SimpleLogger {
	return &SimpleLogger{
		logger: log.New(out, prefix, flag), LogLevel: logLevel}
}

func (s *SimpleLogger) Err(m string) (err error) {
	if s.LogLevel >= LOG_ERR {
		s.logger.Println("[error]", m)
	}
	return
}

func (s *SimpleLogger) Warning(m string) (err error) {
	if s.LogLevel >= LOG_WARNING {
		s.logger.Println("[warning]", m)
	}
	return
}

func (s *SimpleLogger) Info(m string) (err error) {
	if s.LogLevel >= LOG_INFO {
		s.logger.Println("[info]", m)
	}
	return
}

func (s *SimpleLogger) Debug(m string) (err error) {
	if s.LogLevel >= LOG_DEBUG {
		s.logger.Println("[debug]", m)
	}
	return
}
