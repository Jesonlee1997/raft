package util

import (
	"testing"
	"fmt"
)

func TestString(t *testing.T) {
	type Person struct {
		name string
		age int
	}
	p1 := Person{name:"p1", age:11}
	fmt.Println(String(p1))
}