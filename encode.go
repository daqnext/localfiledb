package meson_bolt_localdb

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"math"
)

// EncodeFunc is a function for encoding a value into bytes
type EncodeFunc func(value interface{}) ([]byte, error)

// DecodeFunc is a function for decoding a value from bytes
type DecodeFunc func(data []byte, value interface{}) error

// DefaultEncode is the default encoding func for bolthold (Gob)
func DefaultEncode(value interface{}) ([]byte, error) {

	var b []byte

	isNumber := false
	isNegative := false
	switch value.(type) {
	//case time.Time:
	//case big.Float:
	//case big.Int:
	//case big.Rat:

	case int:
		isNumber = true
		v := value.(int)
		if v < 0 {
			isNegative = true
		}
		b = Int64ToBytes(int64(v))
	case int8:
		isNumber = true
		v := value.(int8)
		if v < 0 {
			isNegative = true
		}
		b = Int64ToBytes(int64(v))
	case int16:
		isNumber = true
		v := value.(int16)
		if v < 0 {
			isNegative = true
		}
		b = Int64ToBytes(int64(v))
	case int32:
		isNumber = true
		v := value.(int32)
		if v < 0 {
			isNegative = true
		}
		b = Int64ToBytes(int64(v))
	case int64:
		isNumber = true
		v := value.(int64)
		if v < 0 {
			isNegative = true
		}
		b = Int64ToBytes(int64(v))

	case float32:
		isNumber = true
		v := value.(float32)
		vi := int64(v * 10000000000)
		if v < 0 {
			isNegative = true
		}
		b = Float32ToByte(v)
		vib := Int64ToBytes(vi)
		b = append(vib, b...)
	case float64:
		isNumber = true
		v := value.(float64)
		vi := int64(v * 10000000000)
		if v < 0 {
			isNegative = true
		}
		b = Float64ToByte(v)
		vib := Int64ToBytes(vi)
		b = append(vib, b...)

	default:
		var buff bytes.Buffer
		en := gob.NewEncoder(&buff)

		err := en.Encode(value)
		if err != nil {
			return nil, err
		}
		return buff.Bytes(), nil

	}

	if !isNumber {
		return b, nil
	}

	if isNegative {
		b = append([]byte{1}, b...)
	} else {
		b = append([]byte{2}, b...)
	}

	return b, nil
}

// DefaultDecode is the default decoding func for bolthold (Gob)
func DefaultDecode(data []byte, value interface{}) error {

	switch value.(type) {
	//case *time.Time:
	//case *big.Float:
	//case *big.Int:
	//case *big.Rat:

	case *int:
		data = data[1:]
		v := value.(*int)
		*v = int(BytesToInt64(data))
		return nil
	case *int8:
		data = data[1:]
		v := value.(*int8)
		*v = int8(BytesToInt64(data))
		return nil
	case *int16:
		data = data[1:]
		v := value.(*int16)
		*v = int16(BytesToInt64(data))
		return nil
	case *int32:
		data = data[1:]
		v := value.(*int32)
		*v = int32(BytesToInt64(data))
		return nil
	case *int64:
		data = data[1:]
		v := value.(*int64)
		*v = int64(BytesToInt64(data))
		return nil
	case *float32:
		data = data[9:]
		v := value.(*float32)
		*v = ByteToFloat32(data)
		return nil

	case *float64:
		data = data[9:]
		v := value.(*float64)
		*v = ByteToFloat64(data)
		return nil

	default:
		var buff bytes.Buffer
		de := gob.NewDecoder(&buff)

		_, err := buff.Write(data)
		if err != nil {
			return err
		}

		return de.Decode(value)

	}

	return nil

}

func Int64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

func BytesToInt64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}

func Float32ToByte(float float32) []byte {
	bits := math.Float32bits(float)
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, bits)

	return bytes
}

func ByteToFloat32(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)

	return math.Float32frombits(bits)
}

func Float64ToByte(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)

	return bytes
}

func ByteToFloat64(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)

	return math.Float64frombits(bits)
}
