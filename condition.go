package localfiledb

import (
	"bytes"
	"errors"
	"reflect"
)

type Operator int

//const (
////opEq Operator = iota
////OpGt
////OpGe
////OpLt
////OpLe
//)

const rangeStart = "RangeStart"
const rangeEnd = "RangeEnd"

type QueryType int

const QueryRange QueryType = 1
const QueryEqual QueryType = 2

type EqualCondition struct {
	//op    Operator
	value interface{}
	Err   error
}

type RangeCondition struct {
	RangePair []*ValuePair
	ValueType reflect.Type
	Err       error
}
type ValuePair struct {
	LeftValueByte  []byte
	IsLeftInclude  bool
	RightValueByte []byte
	IsRightInclude bool
}

func (vp *ValuePair) leftIsStart() bool {
	if bytes.Equal(vp.LeftValueByte, []byte(rangeStart)) {
		return true
	}
	return false
}

func (vp *ValuePair) rightIsEnd() bool {
	if bytes.Equal(vp.RightValueByte, []byte(rangeEnd)) {
		return true
	}
	return false
}

func Lt(value interface{}) *RangeCondition {
	bc := &ValuePair{}
	v, err := DefaultEncode(value)
	if err != nil {
		//save error
		return &RangeCondition{
			RangePair: []*ValuePair{}, //empty RangeCondition
			Err:       err,
		}
	}

	//bc.LeftValue = rangeStart
	bc.LeftValueByte = []byte(rangeStart)
	bc.IsLeftInclude = true

	//bc.RightValue = value
	bc.RightValueByte = v
	bc.IsRightInclude = false

	return &RangeCondition{
		RangePair: []*ValuePair{bc},
		ValueType: reflect.TypeOf(value),
	}
}

func Le(value interface{}) *RangeCondition {
	bc := &ValuePair{}
	v, err := DefaultEncode(value)
	if err != nil {
		//save error
		return &RangeCondition{
			RangePair: []*ValuePair{}, //empty RangeCondition
			Err:       err,
		}
	}

	//bc.LeftValue = rangeStart
	bc.LeftValueByte = []byte(rangeStart)
	bc.IsLeftInclude = true

	//bc.RightValue = value
	bc.RightValueByte = v
	bc.IsRightInclude = true

	return &RangeCondition{
		RangePair: []*ValuePair{bc},
		ValueType: reflect.TypeOf(value),
	}
}

func Gt(value interface{}) *RangeCondition {
	bc := &ValuePair{}
	lv, err := DefaultEncode(value)
	if err != nil {
		//save error
		return &RangeCondition{
			RangePair: []*ValuePair{}, //empty RangeCondition
			Err:       err,
		}
	}

	//bc.LeftValue = value
	bc.LeftValueByte = lv
	bc.IsLeftInclude = false

	//bc.RightValue=rangeEnd
	bc.RightValueByte = []byte(rangeEnd)
	bc.IsRightInclude = true

	return &RangeCondition{
		RangePair: []*ValuePair{bc},
		ValueType: reflect.TypeOf(value),
	}
}

func Ge(value interface{}) *RangeCondition {
	bc := &ValuePair{}
	lv, err := DefaultEncode(value)
	if err != nil {
		//save error
		return &RangeCondition{
			RangePair: []*ValuePair{}, //empty RangeCondition
			Err:       err,
		}
	}

	//bc.LeftValue = value
	bc.LeftValueByte = lv
	bc.IsLeftInclude = true

	//bc.RightValue=rangeEnd
	bc.RightValueByte = []byte(rangeEnd)
	bc.IsRightInclude = true

	return &RangeCondition{
		RangePair: []*ValuePair{bc},
		ValueType: reflect.TypeOf(value),
	}
}

func VPair(min interface{}, minInclude bool, max interface{}, maxInclude bool) *RangeCondition {
	bc := &ValuePair{}
	var t reflect.Type
	if min == nil {
		//bc.LeftValue=rangeStart
		bc.LeftValueByte = []byte(rangeStart)
		bc.IsLeftInclude = true
	} else {
		lv, err := DefaultEncode(min)
		if err != nil {
			//save error
			return &RangeCondition{
				RangePair: []*ValuePair{}, //empty RangeCondition
				Err:       err,
			}
		}
		//bc.LeftValue = min
		bc.LeftValueByte = lv
		bc.IsLeftInclude = minInclude
		t = reflect.TypeOf(min)
	}

	if max == nil {
		//bc.RightValue=rangeEnd
		bc.RightValueByte = []byte(rangeEnd)
		bc.IsRightInclude = true
	} else {
		rv, err := DefaultEncode(max)
		if err != nil {
			//save error
			return &RangeCondition{
				RangePair: []*ValuePair{}, //empty RangeCondition
				Err:       err,
			}
		}
		//bc.RightValue=max
		bc.RightValueByte = rv
		bc.IsRightInclude = maxInclude
		t = reflect.TypeOf(max)
	}

	if min != nil && max != nil {
		if reflect.TypeOf(min) != reflect.TypeOf(max) {
			return &RangeCondition{
				RangePair: []*ValuePair{}, //empty RangeCondition
				Err:       errors.New("query vpair value type different"),
			}
		}
		if bytes.Compare(bc.LeftValueByte, bc.RightValueByte) > 0 {
			return &RangeCondition{
				RangePair: []*ValuePair{}, //empty RangeCondition
			}
		}

		if bytes.Compare(bc.LeftValueByte, bc.RightValueByte) == 0 && (!bc.IsRightInclude || !bc.IsLeftInclude) {
			return &RangeCondition{
				RangePair: []*ValuePair{}, //empty RangeCondition
			}
		}

	}

	return &RangeCondition{
		RangePair: []*ValuePair{bc},
		ValueType: t,
	}
}

func (bc *RangeCondition) And(b *RangeCondition) *RangeCondition {
	if bc.Err != nil {
		return &RangeCondition{
			RangePair: []*ValuePair{}, //empty RangeCondition
			Err:       bc.Err,
		}
	}

	if b == nil {
		return &RangeCondition{
			RangePair: []*ValuePair{}, //empty RangeCondition
		}
	}

	if b.Err != nil {
		return &RangeCondition{
			RangePair: []*ValuePair{}, //empty RangeCondition
			Err:       b.Err,
		}
	}

	if bc.ValueType != b.ValueType {
		return &RangeCondition{
			RangePair: []*ValuePair{}, //empty RangeCondition
			Err:       errors.New("condition value type different"),
		}
	}

	if len(bc.RangePair) == 0 || len(b.RangePair) == 0 {
		return &RangeCondition{
			RangePair: []*ValuePair{}, //empty RangeCondition
		}
	}

	resultPair := []*ValuePair{}

	for _, v1 := range bc.RangePair {
		for _, v2 := range b.RangePair {
			r := v1.and(v2)
			if len(r.RangePair) > 0 {
				resultPair = append(resultPair, r.RangePair...)
			}
		}
	}

	vType := bc.ValueType
	if vType == nil {
		vType = b.ValueType
	}

	return &RangeCondition{
		RangePair: resultPair,
		ValueType: vType,
	}
}

// r1 r2 have no cover range,
// if r1 is smaller than r2 return -1,
// if r1 is bigger than r2 return 1.
func vPairCompare(r1, r2 *ValuePair) int {
	if r1.rightIsEnd() {
		return 1
	}

	if r2.rightIsEnd() {
		return -1
	}

	if r1.leftIsStart() {
		return -1
	}

	if r2.leftIsStart() {
		return 1
	}

	//r1.right  r2.left
	if bytes.Compare(r1.RightValueByte, r2.LeftValueByte) < 0 {
		return -1
	}

	//r2.right r1.left
	if bytes.Compare(r2.RightValueByte, r1.LeftValueByte) < 0 {
		return 1
	}

	//never happen
	return 0
}

func (bc *RangeCondition) Or(b *RangeCondition) *RangeCondition {
	if bc.Err != nil {
		return &RangeCondition{
			RangePair: []*ValuePair{}, //empty RangeCondition
			Err:       bc.Err,
		}
	}

	if b == nil {
		return bc
	}

	if b.Err != nil {
		return &RangeCondition{
			RangePair: []*ValuePair{}, //empty RangeCondition
			Err:       b.Err,
		}
	}

	if len(bc.RangePair) == 0 {
		return b
	}
	if len(b.RangePair) == 0 {
		return bc
	}

	if bc.ValueType != b.ValueType {
		return &RangeCondition{
			RangePair: []*ValuePair{}, //empty RangeCondition
			Err:       errors.New("condition value type different"),
		}
	}

	index1 := 0
	index2 := 0
	tempR := []*ValuePair{}
	for {
		var p1, p2 *ValuePair
		if index1 < len(bc.RangePair) {
			p1 = bc.RangePair[index1]
		}
		if index2 < len(b.RangePair) {
			p2 = b.RangePair[index2]
		}

		if p1 == nil && p2 == nil {
			break
		} else if p1 == nil && p2 != nil {
			tempR = append(tempR, p2)
			index2++
			continue
		} else if p1 != nil && p2 == nil {
			tempR = append(tempR, p1)
			index1++
			continue
		}

		//is p1 and p2 has cover range
		r := p1.and(p2)
		if len(r.RangePair) == 0 {
			//check which one is smaller
			if vPairCompare(p1, p2) < 0 {
				//p1 is smaller
				tempR = append(tempR, p1)
				index1++
				continue
			} else {
				//p2 is smaller
				tempR = append(tempR, p2)
				index2++
				continue
			}

		}

		intersection := p1.or(p2)
		if len(intersection.RangePair) == 0 {
			// should not happen
			index1++
			index2++
			continue
		}

		tempR = append(tempR, intersection.RangePair[0])
		index1++
		index2++
	}

	resultRange := []*ValuePair{}
	tempVPair := tempR[0]
	for i := 1; i < len(tempR); i++ {
		r := tempVPair.or(tempR[i])
		if len(r.RangePair) == 2 {
			resultRange = append(resultRange, r.RangePair[0])
		}
		tempVPair = r.RangePair[len(r.RangePair)-1]
	}

	resultRange = append(resultRange, tempVPair)
	return &RangeCondition{
		RangePair: resultRange,
		ValueType: bc.ValueType,
	}
}

func (vp *ValuePair) or(p *ValuePair) *RangeCondition {
	if p == nil {
		return &RangeCondition{
			RangePair: []*ValuePair{vp}, //empty RangeCondition
		}
	}

	// v1 left should <= v2 left
	v1 := vp
	v2 := p
	if v1.leftIsStart() {
		//do nothing
	} else if !v1.leftIsStart() && v2.leftIsStart() {
		//swap
		v1, v2 = v2, v1
	} else if bytes.Compare(v1.LeftValueByte, v2.LeftValueByte) > 0 {
		v1, v2 = v2, v1
	}

	if !v1.rightIsEnd() && !v2.leftIsStart() {
		if bytes.Compare(v1.RightValueByte, v2.LeftValueByte) < 0 {
			return &RangeCondition{
				RangePair: []*ValuePair{v1, v2},
			}
		}

		if bytes.Compare(v1.RightValueByte, v2.LeftValueByte) == 0 && (v1.IsRightInclude == false && v2.IsLeftInclude == false) {
			return &RangeCondition{
				RangePair: []*ValuePair{v1, v2},
			}
		}
	}

	newPair := &ValuePair{}
	//newPair.LeftValue = v1.LeftValue
	newPair.LeftValueByte = v1.LeftValueByte
	newPair.IsLeftInclude = v1.IsLeftInclude

	// if v1.left==v2.left but all not start
	if bytes.Compare(v1.LeftValueByte, v2.LeftValueByte) == 0 && (!v1.leftIsStart() && !v2.leftIsStart()) {
		if v1.IsLeftInclude || v2.IsLeftInclude {
			newPair.IsLeftInclude = true
		} else {
			newPair.IsLeftInclude = false
		}
	}

	if v2.rightIsEnd() {
		//newPair.RightValue = v2.RightValue
		newPair.RightValueByte = v2.RightValueByte
		newPair.IsRightInclude = v2.IsRightInclude

		return &RangeCondition{
			RangePair: []*ValuePair{newPair},
		}
	}

	if v1.rightIsEnd() {
		//newPair.RightValue = v1.RightValue
		newPair.RightValueByte = v1.RightValueByte
		newPair.IsRightInclude = v1.IsRightInclude

		return &RangeCondition{
			RangePair: []*ValuePair{newPair},
		}
	}

	result := bytes.Compare(v1.RightValueByte, v2.RightValueByte)
	if result < 0 {
		//newPair.RightValue = v2.RightValue
		newPair.RightValueByte = v2.RightValueByte
		newPair.IsRightInclude = v2.IsRightInclude

		return &RangeCondition{
			RangePair: []*ValuePair{newPair},
		}
	} else if result > 0 {
		//newPair.RightValue = v1.RightValue
		newPair.RightValueByte = v1.RightValueByte
		newPair.IsRightInclude = v1.IsRightInclude

		return &RangeCondition{
			RangePair: []*ValuePair{newPair},
		}
	} else {
		//newPair.RightValue = v1.RightValue
		if v1.IsRightInclude || v2.IsRightInclude {
			newPair.IsRightInclude = true
		} else {
			newPair.IsRightInclude = false
		}

		return &RangeCondition{
			RangePair: []*ValuePair{newPair},
		}
	}

}

func (vp *ValuePair) and(p *ValuePair) *RangeCondition {
	if p == nil {
		return &RangeCondition{
			RangePair: []*ValuePair{}, //empty RangeCondition
		}
	}

	// v1 left should <= v2 left
	v1 := vp
	v2 := p
	if v1.leftIsStart() {
		//do nothing
	} else if !v1.leftIsStart() && v2.leftIsStart() {
		//swap
		v1, v2 = v2, v1
	} else if bytes.Compare(v1.LeftValueByte, v2.LeftValueByte) > 0 {
		v1, v2 = v2, v1
	}

	//
	if (!v1.rightIsEnd() && !v2.leftIsStart()) && bytes.Compare(v1.RightValueByte, v2.LeftValueByte) < 0 {
		return &RangeCondition{
			RangePair: []*ValuePair{}, //empty RangeCondition
		}
	}

	newPair := &ValuePair{}
	//newPair.LeftValue = v2.LeftValue
	newPair.LeftValueByte = v2.LeftValueByte
	newPair.IsLeftInclude = v2.IsLeftInclude

	// if v1.left==v2.left but all not start
	if bytes.Compare(v1.LeftValueByte, v2.LeftValueByte) == 0 && (!v1.leftIsStart() && !v2.leftIsStart()) {
		if v1.IsLeftInclude && v2.IsLeftInclude {
			newPair.IsLeftInclude = true
		} else {
			newPair.IsLeftInclude = false
		}
	}

	if v2.rightIsEnd() {
		//newPair.RightValue = v1.RightValue
		newPair.RightValueByte = v1.RightValueByte
		newPair.IsRightInclude = v1.IsRightInclude

		if bytes.Compare(newPair.LeftValueByte, newPair.RightValueByte) == 0 && (newPair.IsLeftInclude == false || newPair.IsRightInclude == false) {
			return &RangeCondition{
				RangePair: []*ValuePair{},
			}
		}
		return &RangeCondition{
			RangePair: []*ValuePair{newPair},
		}
	}

	if v1.rightIsEnd() {
		//newPair.RightValue = v2.RightValue
		newPair.RightValueByte = v2.RightValueByte
		newPair.IsRightInclude = v2.IsRightInclude

		if bytes.Compare(newPair.LeftValueByte, newPair.RightValueByte) == 0 && (newPair.IsLeftInclude == false || newPair.IsRightInclude == false) {
			return &RangeCondition{
				RangePair: []*ValuePair{},
			}
		}
		return &RangeCondition{
			RangePair: []*ValuePair{newPair},
		}
	}

	result := bytes.Compare(v1.RightValueByte, v2.RightValueByte)
	if result < 0 {
		//newPair.RightValue = v1.RightValue
		newPair.RightValueByte = v1.RightValueByte
		newPair.IsRightInclude = v1.IsRightInclude

		if bytes.Compare(newPair.LeftValueByte, newPair.RightValueByte) == 0 && (newPair.IsLeftInclude == false || newPair.IsRightInclude == false) {
			return &RangeCondition{
				RangePair: []*ValuePair{},
			}
		}
		return &RangeCondition{
			RangePair: []*ValuePair{newPair},
		}
	} else if result > 0 {
		//newPair.RightValue = v2.RightValue
		newPair.RightValueByte = v2.RightValueByte
		newPair.IsRightInclude = v2.IsRightInclude

		if bytes.Compare(newPair.LeftValueByte, newPair.RightValueByte) == 0 && (newPair.IsLeftInclude == false || newPair.IsRightInclude == false) {
			return &RangeCondition{
				RangePair: []*ValuePair{},
			}
		}
		return &RangeCondition{
			RangePair: []*ValuePair{newPair},
		}
	} else {
		//newPair.RightValue = v1.RightValue
		if v1.IsRightInclude && v2.IsRightInclude {
			newPair.IsRightInclude = true
		} else {
			newPair.IsRightInclude = false
		}

		if bytes.Compare(newPair.LeftValueByte, newPair.RightValueByte) == 0 && (newPair.IsLeftInclude == false || newPair.IsRightInclude == false) {
			return &RangeCondition{
				RangePair: []*ValuePair{},
			}
		}
		return &RangeCondition{
			RangePair: []*ValuePair{newPair},
		}
	}
}
