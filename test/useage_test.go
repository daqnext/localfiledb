package test

import (
	"fmt"
	"go.etcd.io/bbolt"
	"log"
	"math/rand"
	"os"
	"testing"

	mesondb "github.com/daqnext/meson-bolt-localdb"
)

var store *mesondb.Store

type FileInfoWithIndex struct {
	HashKey        string `boltholdKey:"HashKey"`
	BindName       string `boltholdIndex:"BindName"`
	LastAccessTime int64  `boltholdIndex:"LastAccessTime"`
	FileSize       int64
}

func Test_usePrimaryKey(t *testing.T) {
	os.Remove("test.db")
	var err error
	store, err = mesondb.Open("test.db", 0666, nil)
	if err != nil {
		fmt.Println("bolthold can't open")
	}

	store.Bolt().Batch(func(tx *bbolt.Tx) error {
		for i := 0; i < 100; i++ {
			//hashKey := GenRandomKey(16)
			hashKey := fmt.Sprintf("%d", i)
			bindName := fmt.Sprintf("bindname-%01d", rand.Intn(10)+4)
			fileInfo := FileInfoWithIndex{hashKey, bindName, int64(rand.Intn(100)), int64(rand.Intn(100))}

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

	q := mesondb.NewQuery(mesondb.Key).Range(mesondb.Condition(mesondb.OpGe, "22"), mesondb.Condition(mesondb.OpLe, "30")).Desc().Limit(2).Offset(2)
	//q:=mesondb.NewQuery(mesondb.Key).Equal("22").Limit(2)
	var result []FileInfoWithIndex
	err = store.Find(&result, q)
	if err != nil {
		log.Println(err)
	}
	for _, v := range result {
		log.Println(v)
	}

}

func Test_usage(t *testing.T) {

	os.Remove("test.db")
	var err error
	store, err = mesondb.Open("test.db", 0666, nil)
	if err != nil {
		fmt.Println("bolthold can't open")
	}

	store.Bolt().Batch(func(tx *bbolt.Tx) error {
		for i := 0; i < 100; i++ {
			//hashKey := GenRandomKey(16)
			hashKey := fmt.Sprintf("%d", i)
			bindName := fmt.Sprintf("bindname-%01d", rand.Intn(10)+4)
			fileInfo := FileInfoWithIndex{hashKey, bindName, int64(rand.Intn(100)), int64(rand.Intn(100))}

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

	q := mesondb.NewQuery("LastAccessTime").Range(mesondb.Condition(mesondb.OpGe, 22), mesondb.Condition(mesondb.OpLe, 91)).Desc().Limit(2).Offset(0)

	store.UpdateMatching(&FileInfoWithIndex{}, q, func(record interface{}) error {
		log.Println("before update", record)
		record.(*FileInfoWithIndex).FileSize = 200

		return nil
	})

	store.DeleteMatching(&FileInfoWithIndex{}, q)

	store.Bolt().View(func(tx *bbolt.Tx) error {
		var result []FileInfoWithIndex
		q := mesondb.NewQuery("BindName").Equal("bindname-0")
		err := store.Find(&result, q)
		if err != nil {
			log.Println(err)
		}
		log.Println(result)

		q = mesondb.NewQuery("LastAccessTime").Range(mesondb.Condition(mesondb.OpGe, 22), mesondb.Condition(mesondb.OpLe, 91)).Limit(0).Offset(0)

		err = store.Find(&result, q)
		if err != nil {
			log.Println(err)
		}
		log.Println(len(result))
		for i, v := range result {
			log.Println(i+1, v)
		}

		count, err := store.Count(&FileInfoWithIndex{}, q)
		if err != nil {
			log.Println(err)
		}
		log.Println(count)

		//q=bolthold.NewQuery("LastAccessTime").Range(bolthold.Condition(bolthold.OpGe,22),bolthold.Condition(bolthold.OpLe,91)).Offset(5).Limit(100).Desc()
		//err=store.Find(&result,q)
		//if err != nil {
		//	log.Println(err)
		//}
		//log.Println(len(result))
		//for i,v:=range result{
		//	log.Println(i+1, v)
		//}

		//q=bolthold.NewQuery("LastAccessTime").Range(bolthold.Condition(bolthold.OpGe,22),bolthold.Condition(bolthold.OpLe,53)).Desc()
		//err=store.FindOne(&result,q)
		//if err != nil {
		//	log.Println(err)
		//}
		//log.Println(result)

		//q:=bolthold.NewQuery("LastAccessTime").Equal(71)
		//err:=store.MyFind(&result,q)
		//if err != nil {
		//	log.Println(err)
		//}
		//log.Println(result)

		//bk:=tx.Bucket([]byte("_index:FileInfoWithIndex:BindName"))
		//c:=bk.Cursor()
		//for k,v:=c.First();k!=nil;k,v=c.Next(){
		//	var key string
		//	var value [][]byte
		//	bolthold.DefaultDecode(k,&key)
		//	bolthold.DefaultDecode(v,&value)
		//	log.Println("key:",key,"value:",value)
		//}
		//
		//bk:=tx.Bucket([]byte("_index:FileInfoWithIndex:LastAccessTime"))
		//c:=bk.Cursor()
		//for k,v:=c.First();k!=nil;k,v=c.Next(){
		//	var key int64
		//	var value [][]byte
		//	bolthold.DefaultDecode(k,&key)
		//	bolthold.DefaultDecode(v,&value)
		//	log.Println("key:",key,"value:",value)
		//}

		return nil
	})

}
