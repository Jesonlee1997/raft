package log

import "fmt"

const (
	debug = iota
	info
	warn
	_error
)

var level int

func Debug(args ...interface{})  {
	if debug >= level{
		 fmt.Println(args...)
	}
}

func Info(args ...interface{})  {
	if info >= level{
		fmt.Println(args...)
	}
}

func Warn(args ...interface{})  {
	if warn >= level{
		fmt.Println(args...)
	}
}

func Error(args ...interface{})  {
	if _error >= level{
		fmt.Println(args...)
	}
}

func init() {
	level = debug
}
