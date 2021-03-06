package test

import (
	"fmt"
	mesondb "github.com/daqnext/meson-bolt-localdb"
	"go.etcd.io/bbolt"
	"log"
	"math/rand"
	"os"
	"testing"
)

var store *mesondb.Store

type Pointer struct {
	Name string
}

type FileInfoWithIndex struct {
	HashKey        string `boltholdKey:"HashKey"`
	BindName       string `boltholdIndex:"BindName"`
	LastAccessTime int64  `boltholdIndex:"LastAccessTime"`
	FileSize       int64
	Rate           float64 `boltholdIndex:"Rate"`
	P              *Pointer
}

func Test_singleInsert(t *testing.T) {
	os.Remove("test.db")
	var err error

	store, err = mesondb.Open("test.db", 0666, nil)
	if err != nil {
		log.Println("bolthold can't open")
	}

	p := &Pointer{"pointName"}
	fileInfo := FileInfoWithIndex{BindName: "bindName-1", LastAccessTime: int64(rand.Intn(100)), FileSize: int64(rand.Intn(100)), P: p}
	err = store.Insert("1", fileInfo)
	if err != nil {
		log.Println(err)
	}

	fileInfo = FileInfoWithIndex{BindName: "bindName-2", LastAccessTime: int64(rand.Intn(100)), FileSize: int64(rand.Intn(100)), P: p}
	err = store.Insert("2", fileInfo)
	if err != nil {
		log.Println(err)
	}
}

func Test_uniqueIndexInsert(t *testing.T) {
	os.Remove("test.db")
	var err error
	store, err = mesondb.Open("test.db", 0666, nil)
	if err != nil {
		log.Println("bolthold can't open")
	}
	defer store.Close()

	type SomeStruct struct {
		Name string `boltholdIndex:"Name"`
		No   uint64 `boltholdUnique:"No"`
	}

	s := []SomeStruct{
		{"aaa", 1},
		{"bbb", 2},
		{"ccc", 1},
	}
	for i, v := range s {
		err := store.Insert(i, v)
		if err != nil {
			log.Println("insert index ", i, "err", err)
		}
	}

	var ss []SomeStruct
	store.FindOne(&ss, nil)
	for _, v := range ss {
		log.Println(v)
	}
}

func Test_batchInsert(t *testing.T) {
	os.Remove("test.db")
	var err error
	store, err = mesondb.Open("test.db", 0666, nil)
	if err != nil {
		log.Println("bolthold can't open")
	}

	err = store.Bolt().Update(func(tx *bbolt.Tx) error {
		for i := 0; i < 100; i++ {
			hashKey := fmt.Sprintf("%d", i)
			bindName := fmt.Sprintf("bindname-%01d", rand.Intn(10)+4)
			p := &Pointer{"pointName"}
			fileInfo := FileInfoWithIndex{
				BindName:       bindName,
				LastAccessTime: int64(rand.Intn(100) - 50),
				FileSize:       int64(rand.Intn(100)),
				Rate:           float64(rand.Intn(1000))*0.33 - 150,
				P:              p}

			err := store.TxInsert(tx, hashKey, fileInfo)
			if err != nil {
				log.Println(err)
			}
		}
		return nil
	})
	if err != nil {
		log.Println(err)
	}
}

func Test_singleGetByKey(t *testing.T) {
	Test_singleInsert(t)

	var info FileInfoWithIndex
	err := store.Get("1", &info)
	if err != nil {
		log.Println(err)
	}
	log.Println(info)

	var info2 FileInfoWithIndex
	err = store.Get("3", &info2)
	if err != nil {
		log.Println(err)
	}
	log.Println(info2)
}

////Equal
//mesondb.NewQuery("indexFieldName").Equal(someValue).Limit(10).Offset(10)
////Range
//mesondb.NewQuery("indexFieldName").Range(mesondb.Condition(mesondb.OpGe,someValue),mesondb.Condition(mesondb.OpLe,someValue))
////Offset Limit Exclude Desc Asc
//mesondb.NewQuery("indexFieldName").Range(mesondb.Condition(mesondb.OpGe,someValue)).Limit(10).Offset(10).Exclude(v1,v2,..).Desc()
////if you do not define the Range, it will scan all index value
//mesondb.NewQuery("indexFieldName").Limit(10).Offset(10).Exclude(v1,v2,..).Desc()
////Use indexField "mesondb.Key" to query the Key. It also can use Range query if the Key is sortable
//mesondb.NewQuery(mesondb.Key).Range(mesondb.Condition(mesondb.OpGe,someValue))
////Operator
////mesondb.OpGt ">"
////mesondb.OpGe ">="
////mesondb.OpLt "<"
////mesondb.OpLe "<="

func Test_queryGet(t *testing.T) {
	Test_batchInsert(t)

	var q *mesondb.Query
	var err error

	log.Println("query by primary key")
	var infos []FileInfoWithIndex
	//KeyQuery
	q = mesondb.NewQuery(mesondb.Key).Range(mesondb.Condition(mesondb.OpGe, "10"), mesondb.Condition(mesondb.OpLe, "20"))
	err = store.Find(&infos, q)
	if err != nil {
		log.Println(err)
	}
	for _, v := range infos {
		log.Println(v)
	}

	log.Println("query by some index")
	var infos2 []FileInfoWithIndex
	//IndexQuery
	q = mesondb.NewQuery("LastAccessTime").Range(mesondb.Condition(mesondb.OpGe, int64(-20)), mesondb.Condition(mesondb.OpLe, int64(20)))
	err = store.Find(&infos2, q)
	if err != nil {
		log.Println(err)
	}
	for _, v := range infos2 {
		log.Println(v)
	}

	log.Println("query by some index")
	var infos3 []FileInfoWithIndex
	q = mesondb.NewQuery("Rate").Range(mesondb.Condition(mesondb.OpGe, float64(-20)), mesondb.Condition(mesondb.OpLe, float64(20)))
	err = store.Find(&infos3, q)
	if err != nil {
		log.Println(err)
	}
	for _, v := range infos3 {
		log.Println(v)
	}

	log.Println("query by some index without range")
	var infos4 []FileInfoWithIndex
	q = mesondb.NewQuery("Rate").Offset(10).Limit(10)
	err = store.Find(&infos4, q)
	if err != nil {
		log.Println(err)
	}
	for _, v := range infos4 {
		log.Println(v)
	}

}

func Test_updateQuery(t *testing.T) {
	Test_batchInsert(t)

	log.Println("update query")
	q := mesondb.NewQuery("LastAccessTime").Range(mesondb.Condition(mesondb.OpGe, 10), mesondb.Condition(mesondb.OpLe, 20))
	err := store.UpdateMatching(&FileInfoWithIndex{}, q, func(record interface{}) error {
		v, ok := record.(*FileInfoWithIndex)
		if !ok {
			log.Println("interface{} trans error")
		}
		v.FileSize = 999
		return nil
	})
	if err != nil {
		log.Println(err)
	}

	log.Println("query by primary key")
	var infos []FileInfoWithIndex
	q = mesondb.NewQuery(mesondb.Key).Range(mesondb.Condition(mesondb.OpGe, "0"), mesondb.Condition(mesondb.OpLe, "100"))
	err = store.Find(&infos, q)
	if err != nil {
		log.Println(err)
	}
	for _, v := range infos {
		log.Println(v)
	}
}

func Test_deleteByPrimaryKey(t *testing.T) {
	Test_batchInsert(t)

	err := store.Delete("2", &FileInfoWithIndex{})
	if err != nil {
		log.Println(err)
	}

	store.Delete("5", &FileInfoWithIndex{})
	if err != nil {
		log.Println(err)
	}

	log.Println("query by primary key")
	var infos []FileInfoWithIndex
	q := mesondb.NewQuery(mesondb.Key).Range(mesondb.Condition(mesondb.OpGe, "0"), mesondb.Condition(mesondb.OpLe, "100"))
	err = store.Find(&infos, q)
	if err != nil {
		log.Println(err)
	}
	for _, v := range infos {
		log.Println(v)
	}
}

func Test_deleteQuery(t *testing.T) {
	Test_batchInsert(t)

	log.Println("delete query")
	q := mesondb.NewQuery("LastAccessTime").Range(mesondb.Condition(mesondb.OpGe, 10), mesondb.Condition(mesondb.OpLe, 20))
	err := store.DeleteMatching(&FileInfoWithIndex{}, q)
	if err != nil {
		log.Println(err)
	}

	log.Println("query by primary key")
	var infos []FileInfoWithIndex
	q = mesondb.NewQuery(mesondb.Key).Range(mesondb.Condition(mesondb.OpGe, "0"), mesondb.Condition(mesondb.OpLe, "100"))
	err = store.Find(&infos, q)
	if err != nil {
		log.Println(err)
	}
	for _, v := range infos {
		log.Println(v)
	}
}

func Test_checkIndexBucket(t *testing.T) {
	//os.Remove("test.db")
	var err error
	store, err = mesondb.Open("test.db", 0666, nil)
	if err != nil {
		log.Println("bolthold can't open")
	}

	//
	store.Bolt().View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("_index:FileInfoWithIndex:BindName"))
		c := bk.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var key string
			var value [][]byte
			mesondb.DefaultDecode(k, &key)
			mesondb.DefaultDecode(v, &value)
			log.Println("key:", key, "value:", value)
		}

		bk = tx.Bucket([]byte("_index:FileInfoWithIndex:LastAccessTime"))
		c = bk.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var key int64
			var value [][]byte
			mesondb.DefaultDecode(k, &key)
			mesondb.DefaultDecode(v, &value)
			log.Println("key:", key, "value:", value)
		}

		bk = tx.Bucket([]byte("_index:FileInfoWithIndex:Rate"))
		c = bk.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var key float64
			var value [][]byte
			mesondb.DefaultDecode(k, &key)
			mesondb.DefaultDecode(v, &value)
			log.Println("key:", key, "value:", value)
		}

		return nil
	})
}

func Test_useSimpleKeyValue(t *testing.T) {
	var err error
	store, err = mesondb.Open("test.db", 0666, nil)
	if err != nil {
		log.Println("bolthold can't open")
	}

	setV := map[string]int{}
	setV["a"] = 1
	setV["b"] = 2
	setV["c"] = 3

	store.Bolt().Update(func(tx *bbolt.Tx) error {
		bkt, _ := tx.CreateBucketIfNotExists([]byte("kvbuckt"))

		for k, v := range setV {
			vb, err := mesondb.DefaultEncode(v)
			if err != nil {
				log.Println(err)
			} else {
				bkt.Put([]byte(k), vb)
			}
		}
		return nil
	})

	store.Bolt().View(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket([]byte("kvbuckt"))
		keys := []string{"a", "b", "c"}
		for _, v := range keys {
			v1 := bkt.Get([]byte(v))
			var vv1 int
			err := mesondb.DefaultDecode(v1, &vv1)
			if err != nil {
				log.Println(err)
			} else {
				log.Println("key", v, "value", vv1)
			}
		}
		return nil
	})
}

func Test_reindex(t *testing.T) {
	var err error
	store, err = mesondb.Open("test.db", 0666, nil)
	if err != nil {
		log.Println("bolthold can't open")
	}

	err = store.ReIndex(&FileInfoWithIndex{}, nil)
	if err != nil {
		log.Println(err)
	}

	store.RemoveIndex(&FileInfoWithIndex{}, "FileSize")
}
