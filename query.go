package meson_bolt_localdb

import (
	"bytes"
	"errors"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"reflect"
	"strings"
)

type Query struct {
	index      string
	limit      int
	offset     int
	reverse    bool
	excludeKey [][]byte
	isKeyQuery bool

	queryType      QueryType
	rangeCondition *RangeCondition
	equalCondition *EqualCondition
}

func IndexQuery(index string) *Query {
	return &Query{
		index:      index,
		isKeyQuery: false,
	}
}

func KeyQuery() *Query {
	return &Query{
		index:      "",
		isKeyQuery: true,
	}
}

func (q *Query) Range(c *RangeCondition) *Query {

	q.queryType = QueryRange
	q.rangeCondition = c
	return q
}

func (q *Query) Equal(value interface{}) *Query {
	q.queryType = QueryEqual
	q.equalCondition = &EqualCondition{opEq, value}
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
	}

	if (*q).queryType == 0 {
		(*q).queryType = QueryRange
		(*q).rangeCondition = VPair(nil, true, nil, true)
	}

	if (*q).queryType != QueryEqual && (*q).queryType != QueryRange {
		return errors.New("query type error, only Range or Equal supported")
	}

	if (*q).queryType == QueryEqual {
		if (*q).equalCondition == nil {
			return errors.New("equal Criteria is nil")
		}

		if (*q).equalCondition.value == nil {
			return errors.New("equal Criteria value is nil")
		}

	}

	if (*q).queryType == QueryRange {
		if (*q).rangeCondition == nil {
			return errors.New("range is empty")
		}
		if len((*q).rangeCondition.RangePair) == 0 {
			return errors.New("range is empty")
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

func reverse(s []*ValuePair) []*ValuePair {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
	return s
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
		if len(query.rangeCondition.RangePair) == 0 {
			return errors.New("range is empty")
		}

		vPairs := query.rangeCondition.RangePair
		if query.reverse {
			vPairs = reverse(vPairs)
		}

		leftOffset := query.offset
		keyCount := 0
		for _, vPair := range vPairs {
			tempKeys, finish, err := rangeQuery(s, c, isQueryPrimaryKey, query, vPair, &keyCount, &leftOffset)
			if err != nil {
				return err
			}
			keys = append(keys, tempKeys...)
			if query.limit > 0 && len(keys) >= query.limit {
				break
			}
			if finish {
				break
			}
		}
	case QueryEqual:
		var err error
		keys, err = equalQuery(s, c, isQueryPrimaryKey, query)
		if err != nil {
			return err
		}
	}

	return action(keys, tp, mainBkt)
}

func equalQuery(s *Store, c *bolt.Cursor, isQueryPrimaryKey bool, query *Query) (keyList, error) {
	var keys = make(keyList, 0)
	seek, err := s.encode(query.equalCondition.value)
	if err != nil {
		return nil, fmt.Errorf("query value encode err:%s", err.Error())
	}

	key, v := c.Seek(seek)
	//query value not exist
	if key == nil || v == nil {
		return keys, nil
	}
	if bytes.Compare(key, seek) != 0 {
		return keys, nil
	}

	if isQueryPrimaryKey {
		keys = append(keys, key)
	} else {
		err = s.decode(v, &keys)
		if err != nil {
			return keys, err
		}
	}

	//handle offset
	if query.offset > 0 {
		if query.offset < len(keys) {
			keys = keys[query.offset:]
		} else {
			return keys, nil
		}
	}

	//handle limit
	if query.limit > 0 && query.limit < len(keys) {
		keys = keys[:query.limit]
	}
	return keys, nil
}

func rangeQuery(s *Store, c *bolt.Cursor, isQueryPrimaryKey bool, query *Query, pair *ValuePair, keyCount *int, leftOffset *int) (keyList, bool, error) {
	var keys = make(keyList, 0)

	var forStart func(c *bolt.Cursor) ([]byte, []byte)
	var forCondition func(k []byte) bool
	var forNext func(c *bolt.Cursor) ([]byte, []byte)

	if query.reverse {
		//from right => left
		forStart = func(c *bolt.Cursor) ([]byte, []byte) {
			if pair.rightIsEnd() {
				return c.Last()
			}

			k, v := c.Seek(pair.RightValue)
			if !pair.IsRightInclude && bytes.Equal(pair.RightValue, k) {
				k, v = c.Prev()
			}
			return k, v
		}

		forCondition = func(k []byte) bool {
			if k == nil {
				return false
			}
			if pair.leftIsStart() {
				return k != nil
			}

			if pair.IsLeftInclude {
				return bytes.Compare(k, pair.LeftValue) >= 0
			} else {
				return bytes.Compare(k, pair.LeftValue) > 0
			}
		}

		forNext = func(c *bolt.Cursor) ([]byte, []byte) {
			return c.Prev()
		}
	} else {
		//from left => right
		forStart = func(c *bolt.Cursor) ([]byte, []byte) {
			if pair.leftIsStart() {
				return c.First()
			}

			k, v := c.Seek(pair.LeftValue)
			if !pair.IsLeftInclude && bytes.Equal(pair.LeftValue, k) {
				k, v = c.Next()
			}
			return k, v
		}

		forCondition = func(k []byte) bool {
			if pair.rightIsEnd() {
				return k != nil
			}

			if pair.IsRightInclude {
				return bytes.Compare(k, pair.RightValue) <= 0
			} else {
				return bytes.Compare(k, pair.RightValue) < 0
			}
		}

		forNext = func(c *bolt.Cursor) ([]byte, []byte) {
			return c.Next()
		}
	}

	var k, v []byte
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
			*keyCount++
			//offset
			if query.offset > 0 && *keyCount <= query.offset {
				continue
			}

			keys = append(keys, k)
			//limit
			if query.limit > 0 && len(keys) >= query.limit {
				return keys, true, nil
			}
		} else {
			var tempKeysThisRound = make(keyList, 0)
			err := s.decode(v, &tempKeysThisRound)
			if err != nil {
				return nil, false, err
			}

			//offset
			left := *leftOffset - len(tempKeysThisRound)
			if left >= 0 {
				*leftOffset = left
				continue
			}

			if *leftOffset > 0 {
				tempKeysThisRound = tempKeysThisRound[*leftOffset:]
				*leftOffset = 0
			}

			//limit
			limitThisRound := query.limit - *keyCount
			*keyCount += len(tempKeysThisRound)
			if query.limit > 0 && len(tempKeysThisRound) > limitThisRound {
				tempKeysThisRound = tempKeysThisRound[:limitThisRound]
			}

			keys = append(keys, tempKeysThisRound...)
			if query.limit > 0 && *keyCount >= query.limit {
				return keys, true, nil
			}
		}
	}

	return keys, false, nil

}
