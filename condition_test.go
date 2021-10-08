package localfiledb

import (
	"bytes"
	"fmt"
	"testing"
)

func Test_pairAndOr(t *testing.T) {
	v1 := &ValuePair{[]byte("1"), true, []byte("4"), true}
	v2 := &ValuePair{[]byte("2"), true, []byte("6"), true}
	r := v1.and(v2)
	r = v1.or(v2)
	//log.Println(r)

	v1 = &ValuePair{[]byte("2"), true, []byte("6"), true}
	v2 = &ValuePair{[]byte("1"), true, []byte("4"), true}
	r = v1.and(v2)
	r = v1.or(v2)
	//log.Println(r)

	v1 = &ValuePair{[]byte("1"), true, []byte("6"), true}
	v2 = &ValuePair{[]byte("2"), true, []byte("4"), true}
	r = v1.and(v2)
	r = v1.or(v2)

	v1 = &ValuePair{[]byte("2"), true, []byte("4"), true}
	v2 = &ValuePair{[]byte("1"), true, []byte("6"), true}
	r = v1.and(v2)
	r = v1.or(v2)

	v1 = &ValuePair{[]byte("2"), true, []byte("4"), true}
	v2 = &ValuePair{[]byte("2"), true, []byte("6"), true}
	r = v1.and(v2)
	r = v1.or(v2)

	v1 = &ValuePair{[]byte("2"), true, []byte("4"), true}
	v2 = &ValuePair{[]byte("2"), false, []byte("6"), true}
	r = v1.and(v2)
	r = v1.or(v2)

	v1 = &ValuePair{[]byte("2"), true, []byte("4"), true}
	v2 = &ValuePair{[]byte("1"), true, []byte("4"), true}
	r = v1.and(v2)
	r = v1.or(v2)

	v1 = &ValuePair{[]byte("2"), true, []byte("4"), true}
	v2 = &ValuePair{[]byte("1"), true, []byte("4"), false}
	r = v1.and(v2)
	r = v1.or(v2)

	v1 = &ValuePair{[]byte(rangeStart), true, []byte("4"), true}
	v2 = &ValuePair{[]byte("1"), true, []byte("4"), false}
	r = v1.and(v2)
	r = v1.or(v2)

	v1 = &ValuePair{[]byte(rangeStart), true, []byte("4"), true}
	v2 = &ValuePair{[]byte("1"), true, []byte(rangeEnd), true}
	r = v1.and(v2)
	r = v1.or(v2)

	v1 = &ValuePair{[]byte(rangeStart), true, []byte("4"), true}
	v2 = &ValuePair{[]byte(rangeStart), true, []byte(rangeEnd), true}
	r = v1.and(v2)
	r = v1.or(v2)

	v1 = &ValuePair{[]byte("2"), true, []byte(rangeEnd), true}
	v2 = &ValuePair{[]byte(rangeStart), true, []byte(rangeEnd), true}
	r = v1.and(v2)
	r = v1.or(v2)

	v1 = &ValuePair{[]byte("2"), true, []byte("4"), true}
	v2 = &ValuePair{[]byte(rangeStart), true, []byte(rangeEnd), true}
	r = v1.and(v2)
	r = v1.or(v2)

	v1 = &ValuePair{[]byte("2"), true, []byte("4"), true}
	v2 = &ValuePair{[]byte("6"), true, []byte("8"), true}
	r = v1.and(v2)
	r = v1.or(v2)

	v1 = &ValuePair{[]byte(rangeStart), true, []byte("4"), true}
	v2 = &ValuePair{[]byte("6"), true, []byte("8"), true}
	r = v1.and(v2)
	r = v1.or(v2)

	v1 = &ValuePair{[]byte(rangeStart), true, []byte("4"), true}
	v2 = &ValuePair{[]byte("6"), true, []byte(rangeEnd), true}
	r = v1.and(v2)
	r = v1.or(v2)

	_ = r
}

func (c *RangeCondition) print(name string) {
	fmt.Printf("%s pairs:", name)
	for _, v := range c.RangePair {
		var lv string
		if bytes.Compare([]byte(rangeStart), v.LeftValue) == 0 {
			lv = "start"
		} else {
			DefaultDecode(v.LeftValue, &lv)
		}
		lf := "["
		if v.IsLeftInclude == false {
			lf = "("
		}
		var rv string
		if bytes.Compare([]byte(rangeEnd), v.RightValue) == 0 {
			rv = "end"
		} else {
			DefaultDecode(v.RightValue, &rv)
		}
		rf := "]"
		if v.IsRightInclude == false {
			rf = ")"
		}
		fmt.Printf("%s%s,%s%s", lf, lv, rv, rf)
	}
	fmt.Print("\n")
}

func Test_rangeConditionAndOr(t *testing.T) {
	v1 := VPair("2", false, "8", true).Or(VPair("10", true, "14", true)).Or(VPair("18", true, "20", true)).Or(VPair("24", true, "26", true)).Or(VPair("36", true, "38", true)).Or(VPair("28", true, "30", true))
	v1.print("v1")

	v2 := VPair("0", true, "4", true).Or(VPair("6", true, "12", true)).Or(VPair("16", true, "22", true)).Or(VPair("32", true, "34", true))
	v2.print("v2")

	r := v1.And(v2)
	r.print("r and")

	r = v1.Or(v2)
	r.print("r or")

	_ = r
}
