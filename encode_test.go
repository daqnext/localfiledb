package localfiledb

import (
	"log"
	"testing"
)

func Test_encode(t *testing.T) {
	//int
	vInt := []int{-900, -10, 0, 1, 15, 800}
	for _, v := range vInt {
		value, err := DefaultEncode(v)
		if err != nil {
			log.Println("err:", err)
		} else {
			var ov int
			err := DefaultDecode(value, &ov)
			if err != nil {
				log.Println("err:", err)
			} else {
				log.Println("int decode:", ov)
			}
		}
	}

	//int8
	vInt8 := []int{-100, -10, 0, 1, 15, 80}
	for _, v := range vInt8 {
		value, err := DefaultEncode(v)
		if err != nil {
			log.Println("err:", err)
		} else {
			var ov int8
			err := DefaultDecode(value, &ov)
			if err != nil {
				log.Println("err:", err)
			} else {
				log.Println("int8 decode:", ov)
			}
		}
	}

	//int16
	vInt16 := []int{-900, -10, 0, 1, 15, 800}
	for _, v := range vInt16 {
		value, err := DefaultEncode(v)
		if err != nil {
			log.Println("err:", err)
		} else {
			var ov int16
			err := DefaultDecode(value, &ov)
			if err != nil {
				log.Println("err:", err)
			} else {
				log.Println("int16 decode:", ov)
			}
		}
	}

	//int32
	vInt32 := []int{-900, -10, 0, 1, 15, 800}
	for _, v := range vInt32 {
		value, err := DefaultEncode(v)
		if err != nil {
			log.Println("err:", err)
		} else {
			var ov int32
			err := DefaultDecode(value, &ov)
			if err != nil {
				log.Println("err:", err)
			} else {
				log.Println("int32 decode:", ov)
			}
		}
	}

	//int64
	vInt64 := []int{-900, -10, 0, 1, 15, 800}
	for _, v := range vInt64 {
		value, err := DefaultEncode(v)
		if err != nil {
			log.Println("err:", err)
		} else {
			var ov int64
			err := DefaultDecode(value, &ov)
			if err != nil {
				log.Println("err:", err)
			} else {
				log.Println("int64 decode:", ov)
			}
		}
	}

	//float32
	vFloat32 := []float32{-900.11, -10, 11, 0, 1.11, 15.11, 80.11}
	for _, v := range vFloat32 {
		value, err := DefaultEncode(v)
		if err != nil {
			log.Println("err:", err)
		} else {
			var ov float32
			err := DefaultDecode(value, &ov)
			if err != nil {
				log.Println("err:", err)
			} else {
				log.Println("float32 decode:", ov)
			}
		}
	}

	//float64
	vFloat64 := []float64{-900.11, -10, 11, 0, 1.11, 15.11, 80.11}
	for _, v := range vFloat64 {
		value, err := DefaultEncode(v)
		if err != nil {
			log.Println("err:", err)
		} else {
			var ov float64
			err := DefaultDecode(value, &ov)
			if err != nil {
				log.Println("err:", err)
			} else {
				log.Println("float64 decode:", ov)
			}
		}
	}

	type St struct {
		Name string
		Age  int
		Rate float64
		T    bool
	}
	st := &St{
		Name: "abc",
		Age:  10,
		Rate: 19.87,
		T:    true,
	}
	value, err := DefaultEncode(st)
	if err != nil {
		log.Println("err:", err)
	} else {
		var ov St
		err := DefaultDecode(value, &ov)
		if err != nil {
			log.Println("err:", err)
		} else {
			log.Println("St decode:", ov)
		}
	}
}
