package meson_bolt_localdb

import (
	"bytes"
	"errors"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"reflect"
	"strings"
)

type Operator int

const (
	opEq Operator = iota
	OpGt
	OpGe
	OpLt
	OpLe
)

type QueryType int

const QueryRange QueryType = 1
const QueryEqual QueryType = 2

// Key is shorthand for specifying a query to run again the Key in a bolthold, simply returns ""
// Where(bolthold.Key).Eq("testkey")
const Key = ""

// BoltholdKeyTag is the struct tag used to define an a field as a key for use in a Find query
const BoltholdKeyTag = "boltholdKey"

type Criterion struct {
	op    Operator
	value interface{}
}

type Query struct {
	index      string
	limit      int
	offset     int
	reverse    bool
	excludeKey [][]byte

	queryType     QueryType
	rangeCriteria []*Criterion
	equalCriteria *Criterion
}

func NewQuery(index string) *Query {
	return &Query{
		index: index,
	}
}

func (q *Query) Range(c ...*Criterion) *Query {
	if q.rangeCriteria == nil {
		q.rangeCriteria = []*Criterion{}
	}
	q.queryType = QueryRange
	q.rangeCriteria = append(q.rangeCriteria, c...)
	return q
}

func (q *Query) Equal(value interface{}) *Query {
	q.queryType = QueryEqual
	q.equalCriteria = &Criterion{opEq, value}
	return q
}

func (q *Query) Exclude(value ...interface{}) *Query {
	if q.excludeKey == nil {
		q.excludeKey = [][]byte{}
	}
	for _, v := range value {
		key, err := DefaultEncode(v)
		if err == nil {
			q.excludeKey = append(q.excludeKey, key)
		}
	}
	return q
}

func (q *Query) Limit(limit int) *Query {
	q.limit = limit
	return q
}

func (q *Query) Offset(offset int) *Query {
	q.offset = offset
	return q
}

func (q *Query) Asc() *Query {
	q.reverse = false
	return q
}

func (q *Query) Desc() *Query {
	q.reverse = true
	return q
}

func checkQuery(q **Query) error {
	if *q == nil {
		*q = &Query{}
		//return errors.New("nil query condition")
	}

	if (*q).queryType == 0 {
		(*q).queryType = QueryRange
	}

	if (*q).queryType != QueryEqual && (*q).queryType != QueryRange {
		return errors.New("query type error, only Range or Equal supported")
	}

	if (*q).queryType == QueryEqual {
		if (*q).equalCriteria == nil {
			return errors.New("equal Criteria is nil")
		}

		if (*q).equalCriteria.value == nil {
			return errors.New("equal Criteria value is nil")
		}

	}

	if (*q).queryType == QueryRange {
		if len((*q).rangeCriteria) > 2 {
			return errors.New("range condition error,max condition count is 2")
		}

		var minValue, maxValue []byte
		var err error
		for _, v := range (*q).rangeCriteria {
			if v.value == nil {
				return errors.New("range Criteria value is nil")
			}

			switch v.op {
			case OpGe, OpGt:
				minValue, err = DefaultEncode(v.value)
				if err != nil {
					return err
				}
			case OpLe, OpLt:
				maxValue, err = DefaultEncode(v.value)
				if err != nil {
					return err
				}
			}

		}

		if minValue != nil && maxValue != nil {
			if bytes.Compare(minValue, maxValue) > 0 {
				return errors.New("range Criteria value range error")
			}
		}

	}

	if (*q).limit < 0 {
		return errors.New("limit error")
	}

	if (*q).offset < 0 {
		return errors.New("offset error")
	}

	return nil
}

func Condition(op Operator, value interface{}) *Criterion {
	return &Criterion{
		op:    op,
		value: value,
	}
}

func (s *Store) findOneQuery(source BucketSource, result interface{}, query *Query) error {
	if query == nil {
		query = &Query{}
		query.queryType = QueryRange
		//return errors.New("nil query condition")
	}
	query.Limit(1)
	return s.findQuery(source, result, query)
}

func (s *Store) updateQuery(source BucketSource, dataType interface{}, query *Query, update func(record interface{}) error) error {
	err := checkQuery(&query)
	if err != nil {
		return err
	}

	storer := s.newStorer(dataType)
	return s.runQuery(source, dataType, reflect.TypeOf(dataType), query, func(keys keyList, tp reflect.Type, bkt *bolt.Bucket) error {
		for _, k := range keys {
			v := bkt.Get(k)

			val := reflect.New(tp)
			err := s.decode(v, val.Interface())
			if err != nil {
				return err
			}

			upVal := val.Elem().Interface()

			// delete any existing indexes bad on original value
			err = s.deleteIndexes(storer, source, k, upVal)
			if err != nil {
				return err
			}

			err = update(upVal)
			if err != nil {
				return err
			}

			encVal, err := s.encode(upVal)
			if err != nil {
				return err
			}

			err = bkt.Put(k, encVal)
			if err != nil {
				return err
			}

			// insert any new indexes
			err = s.addIndexes(storer, source, k, upVal)
			if err != nil {
				return err
			}
		}

		return nil

	})
}

func (s *Store) deleteQuery(source BucketSource, dataType interface{}, query *Query) error {
	err := checkQuery(&query)
	if err != nil {
		return err
	}

	storer := s.newStorer(dataType)
	return s.runQuery(source, dataType, reflect.TypeOf(dataType), query, func(keys keyList, tp reflect.Type, bkt *bolt.Bucket) error {
		for _, k := range keys {
			v := bkt.Get(k)

			val := reflect.New(tp)
			err := s.decode(v, val.Interface())
			if err != nil {
				return err
			}

			upVal := val.Elem().Interface()

			err = bkt.Delete(k)
			if err != nil {
				return err
			}

			// remove any indexes
			err = s.deleteIndexes(storer, source, k, upVal)
			if err != nil {
				return err
			}

		}

		return nil
	})

}

func (s *Store) countQuery(source BucketSource, dataType interface{}, query *Query) (int, error) {
	err := checkQuery(&query)
	if err != nil {
		return 0, err
	}

	fixedQuery := *query
	fixedQuery.limit = 0
	fixedQuery.offset = 0
	//check result type
	count := 0
	//run query
	err = s.runQuery(source, dataType, reflect.TypeOf(dataType), &fixedQuery, func(keys keyList, tp reflect.Type, bkt *bolt.Bucket) error {
		count = len(keys)
		return nil
	})

	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Store) findQuery(source BucketSource, result interface{}, query *Query) error {
	err := checkQuery(&query)
	if err != nil {
		return err
	}

	//check result type
	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr || resultVal.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}

	sliceVal := resultVal.Elem()
	elType := sliceVal.Type().Elem()

	resultVal.Elem().Set(sliceVal.Slice(0, 0))

	tp := elType

	for tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}

	//for autofill KeyField in struct
	var keyType reflect.Type
	var keyField string

	for i := 0; i < tp.NumField(); i++ {
		if strings.Contains(string(tp.Field(i).Tag), BoltholdKeyTag) {
			keyType = tp.Field(i).Type
			keyField = tp.Field(i).Name
			break
		}
	}

	val := reflect.New(tp)

	dataType := val.Interface()

	//run query
	return s.runQuery(source, dataType, tp, query, func(keys keyList, tp reflect.Type, bkt *bolt.Bucket) error {
		for _, k := range keys {
			v := bkt.Get(k)

			val := reflect.New(tp)
			err := s.decode(v, val.Interface())
			if err != nil {
				return err
			}
			rowValue := val.Elem()

			if keyType != nil {
				rowKey := rowValue
				for rowKey.Kind() == reflect.Ptr {
					rowKey = rowKey.Elem()
				}
				err := s.decode(k, rowKey.FieldByName(keyField).Addr().Interface())
				if err != nil {
					return err
				}
			}

			sliceVal = reflect.Append(sliceVal, rowValue)
		}
		resultVal.Elem().Set(sliceVal.Slice(0, sliceVal.Len()))
		return nil
	})
}

func (s *Store) runQuery(source BucketSource, dataType interface{}, tp reflect.Type, query *Query, action func(keys keyList, tp reflect.Type, bkt *bolt.Bucket) error) error {
	//run query
	storer := s.newStorer(dataType)
	mainBkt := source.Bucket([]byte(storer.Type()))
	if mainBkt == nil {
		// if the bucket doesn't exist or is empty then our job is really easy!
		return nil
	}

	isQueryPrimaryKey := false
	var queryBkt *bolt.Bucket
	if query.index == "" {
		queryBkt = mainBkt
		isQueryPrimaryKey = true
	} else {
		queryBkt = source.Bucket(indexBucketName(storer.Type(), query.index))
	}
	if query.index != "" && queryBkt == nil {
		return fmt.Errorf("index [%s] does not exist", query.index)
	}

	c := queryBkt.Cursor()
	var keys = make(keyList, 0)

	switch query.queryType {
	case QueryRange:
		if len(query.rangeCriteria) > 2 {
			return errors.New("range condition error,max condition count is 2")
		}

		var forStart func(c *bolt.Cursor) ([]byte, []byte)
		var forCondition func(k []byte) bool
		var forNext func(c *bolt.Cursor) ([]byte, []byte)

		if len(query.rangeCriteria) == 0 {
			if query.reverse {
				forStart = func(c *bolt.Cursor) ([]byte, []byte) {
					return c.Last()
				}
				forCondition = func(k []byte) bool {
					return k != nil
				}
			} else {
				forStart = func(c *bolt.Cursor) ([]byte, []byte) {
					return c.First()
				}
				forCondition = func(k []byte) bool {
					return k != nil
				}
			}
		} else {

			switch query.rangeCriteria[0].op {
			case OpGe:
				seekMin, err := s.encode(query.rangeCriteria[0].value)
				if err != nil {
					return fmt.Errorf("query value encode err:%s", err.Error())
				}

				if query.reverse {
					forStart = func(c *bolt.Cursor) ([]byte, []byte) {
						return c.Last()
					}
					forCondition = func(k []byte) bool {
						return bytes.Compare(k, seekMin) >= 0
					}
				} else {
					forStart = func(c *bolt.Cursor) ([]byte, []byte) {
						return c.Seek(seekMin)
					}
					forCondition = func(k []byte) bool {
						return k != nil
					}
				}

			case OpGt:
				seekMin, err := s.encode(query.rangeCriteria[0].value)
				if err != nil {
					return fmt.Errorf("query value encode err:%s", err.Error())
				}

				if query.reverse {
					forStart = func(c *bolt.Cursor) ([]byte, []byte) {
						return c.Last()
					}
					forCondition = func(k []byte) bool {
						return bytes.Compare(k, seekMin) > 0
					}
				} else {
					forStart = func(c *bolt.Cursor) ([]byte, []byte) {
						k, v := c.Seek(seekMin)
						if bytes.Compare(k, seekMin) == 0 {
							return c.Next()
						}
						return k, v
					}
					forCondition = func(k []byte) bool {
						return k != nil
					}
				}
			case OpLe:
				value, err := s.encode(query.rangeCriteria[0].value)
				if err != nil {
					return fmt.Errorf("query value encode err:%s", err.Error())
				}
				if query.reverse {
					forStart = func(c *bolt.Cursor) ([]byte, []byte) {

						k, v := c.Seek(value)
						if bytes.Compare(k, value) > 0 {
							k, v = c.Prev()
						}

						return k, v

					}
					forCondition = func(k []byte) bool {
						return k != nil
					}

				} else {
					forStart = func(c *bolt.Cursor) ([]byte, []byte) {
						return c.First()
					}
					forCondition = func(k []byte) bool {
						if k == nil {
							return false
						}
						return bytes.Compare(k, value) <= 0
					}
				}

			case OpLt:
				value, err := s.encode(query.rangeCriteria[0].value)
				if err != nil {
					return fmt.Errorf("query value encode err:%s", err.Error())
				}
				if query.reverse {
					forStart = func(c *bolt.Cursor) ([]byte, []byte) {

						k, v := c.Seek(value)
						if bytes.Compare(k, value) >= 0 {
							k, v = c.Prev()
						}

						return k, v

					}
					forCondition = func(k []byte) bool {
						return k != nil
					}

				} else {
					forStart = func(c *bolt.Cursor) ([]byte, []byte) {
						return c.First()
					}
					forCondition = func(k []byte) bool {
						if k == nil {
							return false
						}
						return bytes.Compare(k, value) < 0
					}
				}
			}

			if len(query.rangeCriteria) == 2 {
				switch query.rangeCriteria[1].op {
				case OpGe:
					seekMin, err := s.encode(query.rangeCriteria[1].value)
					if err != nil {
						return fmt.Errorf("query value encode err:%s", err.Error())
					}

					if query.reverse {
						if forStart == nil {
							forStart = func(c *bolt.Cursor) ([]byte, []byte) {
								return c.Last()
							}
						}
						forCondition = func(k []byte) bool {
							return bytes.Compare(k, seekMin) >= 0
						}
					} else {
						forStart = func(c *bolt.Cursor) ([]byte, []byte) {
							return c.Seek(seekMin)
						}
						if forCondition == nil {
							forCondition = func(k []byte) bool {
								return k != nil
							}
						}
					}

				case OpGt:
					seekMin, err := s.encode(query.rangeCriteria[1].value)
					if err != nil {
						return fmt.Errorf("query value encode err:%s", err.Error())
					}

					if query.reverse {
						if forStart == nil {
							forStart = func(c *bolt.Cursor) ([]byte, []byte) {
								return c.Last()
							}
						}
						forCondition = func(k []byte) bool {
							return bytes.Compare(k, seekMin) > 0
						}
					} else {
						forStart = func(c *bolt.Cursor) ([]byte, []byte) {
							k, v := c.Seek(seekMin)
							if bytes.Compare(k, seekMin) == 0 {
								return c.Next()
							}
							return k, v
						}
						if forCondition == nil {
							forCondition = func(k []byte) bool {
								return k != nil
							}
						}
					}

				case OpLe:
					value, err := s.encode(query.rangeCriteria[1].value)
					if err != nil {
						return fmt.Errorf("query value encode err:%s", err.Error())
					}
					if query.reverse {
						forStart = func(c *bolt.Cursor) ([]byte, []byte) {

							k, v := c.Seek(value)
							if bytes.Compare(k, value) > 0 {
								k, v = c.Prev()
							}

							return k, v

						}
						if forCondition == nil {
							forCondition = func(k []byte) bool {
								return k != nil
							}
						}
					} else {
						if forStart == nil {
							forStart = func(c *bolt.Cursor) ([]byte, []byte) {
								return c.First()
							}
						}
						forCondition = func(k []byte) bool {
							if k == nil {
								return false
							}

							return bytes.Compare(k, value) <= 0
						}
					}

				case OpLt:
					value, err := s.encode(query.rangeCriteria[1].value)
					if err != nil {
						return fmt.Errorf("query value encode err:%s", err.Error())
					}

					if query.reverse {
						forStart = func(c *bolt.Cursor) ([]byte, []byte) {

							k, v := c.Seek(value)
							if bytes.Compare(k, value) >= 0 {
								k, v = c.Prev()
							}

							return k, v

						}
						if forCondition == nil {
							forCondition = func(k []byte) bool {
								return k != nil
							}
						}
					} else {
						if forStart == nil {
							forStart = func(c *bolt.Cursor) ([]byte, []byte) {
								return c.First()
							}
						}
						forCondition = func(k []byte) bool {
							if k == nil {
								return false
							}
							return bytes.Compare(k, value) < 0
						}
					}

				}
			}
		}

		if query.reverse {
			forNext = func(c *bolt.Cursor) ([]byte, []byte) {
				return c.Prev()
			}
		} else {
			forNext = func(c *bolt.Cursor) ([]byte, []byte) {
				return c.Next()
			}
		}

		var k, v []byte
		leftOffset := query.offset
		keyCount := 0
		for k, v = forStart(c); forCondition(k); k, v = forNext(c) {
			skip := false
			for _, exclude := range query.excludeKey {
				if bytes.Compare(k, exclude) == 0 {
					skip = true
					break
				}
			}
			if skip {
				continue
			}

			if isQueryPrimaryKey {
				keyCount++
				//offset
				if query.offset > 0 && keyCount <= query.offset {
					continue
				}

				keys = append(keys, k)
				//limit
				if query.limit > 0 && len(keys) >= query.limit {
					break
				}
			} else {
				var tempKeysThisRound = make(keyList, 0)
				err := s.decode(v, &tempKeysThisRound)
				if err != nil {
					return err
				}

				//offset
				left := leftOffset - len(tempKeysThisRound)
				if left >= 0 {
					leftOffset = left
					continue
				}
				if leftOffset > 0 {
					tempKeysThisRound = tempKeysThisRound[leftOffset:]
					leftOffset = 0
				}

				//limit
				limitThisRound := query.limit - keyCount
				keyCount += len(tempKeysThisRound)
				if query.limit > 0 && len(tempKeysThisRound) > limitThisRound {
					tempKeysThisRound = tempKeysThisRound[:limitThisRound]
				}

				keys = append(keys, tempKeysThisRound...)
				if query.limit > 0 && keyCount >= query.limit {
					break
				}
			}
		}
	case QueryEqual:
		seek, err := s.encode(query.equalCriteria.value)
		if err != nil {
			return fmt.Errorf("query value encode err:%s", err.Error())
		}

		key, v := c.Seek(seek)
		//query value not exist
		if key == nil || v == nil {
			return nil
		}
		if bytes.Compare(key, seek) != 0 {
			return nil
		}

		if isQueryPrimaryKey {
			keys = append(keys, key)
		} else {
			err = s.decode(v, &keys)
			if err != nil {
				return err
			}
		}

		//handle offset
		if query.offset > 0 {
			if query.offset < len(keys) {
				keys = keys[query.offset:]
			} else {
				return nil
			}
		}

		//handle limit
		if query.limit > 0 && query.limit < len(keys) {
			keys = keys[:query.limit]
		}

	}

	return action(keys, tp, mainBkt)
}
