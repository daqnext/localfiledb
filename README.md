# meson-bolt-localdb

It's a local file db based on [bblot](https://github.com/etcd-io/bbolt) & [BoltHold](https://github.com/timshannon/bolthold)

## How to use
```
go get github.com/daqnext/meson-bolt-localdb
go get go.etcd.io/bbolt
```

### Open db file
```go
import mesondb "github.com/daqnext/meson-bolt-localdb"
import "go.etcd.io/bbolt"

var store *mesondb.Store
var err error
store, err = mesondb.Open("test.db", 0666, nil)
if err != nil {
    log.Println("bolthold can't open")
	return
}
defer store.Close()
```
use custom option
```go
op:=&mesondb.Options{
    Decoder: func(data []byte, value interface{}) error {
        //define your own decoder

        return nil
    },
    Encoder: func(value interface{}) ([]byte, error) {
        //define your own encoder

        return []byte{},nil
    },
    //other bbolt options
    //...
    Options:&bbolt.Options{},
}
store, err = mesondb.Open("test.db", 0666,op)
```
defalut Decoder and Encoder use "golang/gob" except int (int8 int16...) float32 and float64, number value use the encoder which result []byte can be sorted correctly.

### Define struct
```go
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
```
You can use tag "boltholdIndex","boltholdUnique" to create index. It can be used to do query.
If you use tag "boltholdKey" means this field is the Key for this record in key-value storage

### Insert to db
single insert
```go
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
```

batch insert
```go
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
			P:              p
		}

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
```
if the key is already exist, err ErrKeyExists will return.

Upsert() and TxUpsert() can be used. Upsert() and TxUpsert() inserts the record if it doesn't exist. If it does already exist, then it updates the existing record


### query or get
get by key
```go
var info FileInfoWithIndex
err := store.Get("1", &info)
if err != nil {
	log.Println(err)
}
log.Println(info)

var info2 FileInfoWithIndex
err = store.Get("2", &info2)
if err != nil {
	log.Println(err)
}
log.Println(info2)
```
if this given key not exist, err ErrNotFound will return.

use query
```go
log.Println("query by some index")
var infos []FileInfoWithIndex
q:=mesondb.NewQuery("LastAccessTime").Range(mesondb.Condition(mesondb.OpGe,int64(-20)),mesondb.Condition(mesondb.OpLe,int64(20)))
err:=store.Find(&infos,q)
if err != nil {
	log.Println(err)
}
for _,v:=range infos2{
	log.Println(v)
}
```
query condition example:
```go
//Equal
mesondb.NewQuery("indexFieldName").Equal(someValue)
//Range
mesondb.NewQuery("indexFieldName").Range(mesondb.Condition(mesondb.OpGe,someValue),mesondb.Condition(mesondb.OpLe,someValue))
//Offset Limit Exclude Desc Asc
mesondb.NewQuery("indexFieldName").Range(mesondb.Condition(mesondb.OpGe,someValue)).Limit(10).Offset(10).Exclude(v1,v2,..).Desc()
//if you do not define the Range, it will scan all index value 
mesondb.NewQuery("indexFieldName").Limit(10).Offset(10).Exclude(v1,v2,..).Desc()
//Use indexField "mesondb.Key" to query the Key. It also can use Range query if the Key is sortable
mesondb.NewQuery(mesondb.Key).Range(mesondb.Condition(mesondb.OpGe,someValue))
//Operator
//mesondb.OpGt ">"
//mesondb.OpGe ">="
//mesondb.OpLt "<"
//mesondb.OpLe "<="
```
The Range query can not get the correct result if the index value is not sortable. Do not use Range query with unsortable index or key.

Number and string are sortable with default Encoder.

```go
//if the query is nil, it will get all the value
log.Println("query all value")
var infos []FileInfoWithIndex
err := store.Find(&infos4, nil)
if err != nil {
log.Println(err)
}
for _, v := range infos {
log.Println(v)
}

```

### Update query
```go
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
```

### Delete
by key
```go
// input the Key and dataType
err:=store.Delete("1",&FileInfoWithIndex{})
if err != nil {
	log.Println(err)
}
```

by query
```go
log.Println("delete query")
q := mesondb.NewQuery("LastAccessTime").Range(mesondb.Condition(mesondb.OpGe, 10), mesondb.Condition(mesondb.OpLe, 20))
err := store.DeleteMatching(&FileInfoWithIndex{}, q)
if err != nil {
	log.Println(err)
}
```
