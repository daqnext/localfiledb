package main

import (
	"fmt"
	ldb "github.com/daqnext/localfiledb"
	"go.etcd.io/bbolt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

var store *ldb.Store

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

func mixUsage(round int) {
	for j := 0; j < 500; j++ {
		log.Println("start round ", j+1)
		startTime := time.Now()
		//insert
		err := store.Bolt().Update(func(tx *bbolt.Tx) error {
			for i := 0; i < 10000; i++ {
				hashKey := fmt.Sprintf("%d", rand.Intn(100000000))
				bindName := fmt.Sprintf("bindname-%d", rand.Intn(100000))
				p := &Pointer{"pointName"}
				fileInfo := FileInfoWithIndex{
					BindName:       bindName,
					LastAccessTime: int64(rand.Intn(100000)),
					FileSize:       int64(rand.Intn(1000000)),
					Rate:           float64(rand.Intn(100000))*0.33 - 15000,
					P:              p,
				}

				err := store.TxUpsert(tx, hashKey, fileInfo)
				if err != nil {
					log.Println("TxInsert err", err)
				}
			}
			return nil
		})
		if err != nil {
			log.Println("Update err", err)
		}

		//query
		qc := ldb.Gt(int64(10000)).And(ldb.Lt(int64(20000)))
		q := ldb.IndexQuery("LastAccessTime").Range(qc).Limit(1000).Offset(1000)
		var info []*FileInfoWithIndex
		err = store.Find(&info, q)
		if err != nil {
			log.Println("query find err", err)
		}

		//update
		l := fmt.Sprintf("%d", rand.Intn(1000000)+1000000)
		r := fmt.Sprintf("%d", rand.Intn(2000000)+2000000)
		qc = ldb.Gt(l).And(ldb.Lt(r))
		q = ldb.KeyQuery().Range(qc).Limit(100).Offset(100).Desc()
		err = store.UpdateMatching(&FileInfoWithIndex{}, q, func(record interface{}) error {
			v, ok := record.(*FileInfoWithIndex)
			if !ok {
				log.Println("interface{} trans error")
			}
			v.FileSize = 999
			return nil
		})
		if err != nil {
			log.Println("query UpdateMatching err", err)
		}

		//delete
		q = ldb.IndexQuery("Rate").Range(ldb.Ge(float64(-1000)).And(ldb.Le(float64(1000)))).Limit(100)
		err = store.DeleteMatching(&FileInfoWithIndex{}, q)
		if err != nil {
			log.Println("query DeleteMatching err", err)
		}

		log.Println("round", j+1, "use time", time.Since(startTime).Milliseconds(), "ms")
	}
}

func Test_M() {
	logFile, _ := os.Create("./log")
	log.SetOutput(logFile)

	os.Remove("test.db")
	var err error
	store, err = ldb.Open("test.db", 0666, nil)
	if err != nil {
		log.Println("bolthold can't open")
	}

	wg := &sync.WaitGroup{}
	wg.Add(20)
	for i := 0; i < 20; i++ {
		j := i
		go func() {
			mixUsage(j)
			wg.Done()
		}()
	}

	wg.Wait()
	log.Println("finish")
}

func main() {
	Test_M()
}
