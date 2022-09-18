package db

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"time"

	// "github.com/kamva/mgm/v3"
	// "git.sys-tem.org/caos/db4bigdata/internal/model"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoConfig struct {
	URL      url.URL
	UserName string
	Password string
}

type Database struct {
	conn    *mongo.Client
	db      *mongo.Database
	context *context.Context
}

type abstractStructFieldSet struct {
	fields []abstractStructField
}

type abstractStructField struct {
	key   string
	value reflect.Value
	tp    reflect.StructField
}

// This function is not tested
func Initialize(mongo *Database) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := mongo.db.Drop(ctx)

	if err == nil {
		return true
	}
	return false
}

func ConnectMongo() (*Database, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mongourl := os.Getenv("MONGO_CONNECTION")
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongourl))
	logrus.Println(os.Getenv("MONGO_DB"))
	db := &Database{client, client.Database(os.Getenv("MONGO_DB")), &ctx}
	return db, err
}

func (mongo *Database) Drop(collectionName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := mongo.db.Collection(collectionName).Drop(ctx); err != nil {
		logrus.Errorln(err)
		return err
	}
	return nil
}

// I would call this method import
func (mongo *Database) Save(obj interface{}) error {
	t := getDirectTypeFromInterface(obj)
	coll := mongo.db.Collection(getNestedElemName(t))
	logrus.Println(getNestedElemName(t))
	objs := getInterfaceSliceFromInterface(obj)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := coll.InsertMany(ctx, objs)
	if err != nil {
		logrus.Errorln("Error when inserting objects: ", err)
		return err
	}

	logrus.Printf("Inserted %d documents for Collection %s", len(res.InsertedIDs), getNestedElemName(t))

	return nil
}

// Migrate - does nothing here
func (mongo *Database) Migrate(inf ...interface{}) error {
	return fmt.Errorf("no implementation here")
}

// TODO: implement delete logic
func (mongo *Database) Delete(obj interface{}) error {
	t := getDirectTypeFromInterface(obj)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	coll := mongo.db.Collection(t.Elem().Name())
	deleteResult, err := coll.DeleteMany(ctx, obj)
	if err != nil {
		logrus.Errorln(err)
		return err
	}
	logrus.Debug("Deleted {", deleteResult.DeletedCount, "} objects")

	return nil
}

// Returns sql-Result
func (mongo *Database) Find(qry interface{}, target interface{}) (*mongo.Cursor, error) {
	t := getDirectTypeFromInterface(target)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	logrus.Println(t.Name())
	coll := mongo.db.Collection(t.Name())
	cursor, err := coll.Find(ctx, qry)
	if err != nil {
		logrus.Errorln("Find failed: ", err)
		return nil, err
	}
	// if err = cursor.All(ctx, target); err != nil {
	// 	logrus.Fatal(err)
	// }
	// defer cursor.Close(ctx)

	return cursor, nil
}

func (mongo *Database) Exec(qry string, inf interface{}) error {
	return errors.New("not implemented")
}

func (mongo *Database) Close() error {
	err := mongo.conn.Disconnect(*mongo.context)
	return err
}

func resolveStructFields(inf interface{}) []reflect.StructField {
	strct := getDirectTypeFromInterface(inf)
	fields := []reflect.StructField{}
	for i := 0; i < strct.NumField(); i++ {
		fields = append(fields, strct.Field(i))
	}
	return fields
}

func getAsAbstractStructFieldSetFromInterface(inf interface{}) abstractStructFieldSet {
	fields := resolveStructFields(inf)
	afs := abstractStructFieldSet{}
	for k, field := range fields {
		f := abstractStructField{
			key:   field.Name,
			value: getDirectStructFromInterface(inf).Field(k),
			tp:    getDirectTypeFromInterface(inf).Field(k),
		}
		afs.fields = append(afs.fields, f)
	}
	return afs
}

func getNestedElemName(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Array:
		return getNestedElemName(t.Elem())
	case reflect.Slice:
		return getNestedElemName(t.Elem())
	case reflect.Ptr:
		return getNestedElemName(t.Elem())
	default:
		return t.Name()
	}
}

func getDirectTypeFromInterface(inf interface{}) reflect.Type {
	var tp reflect.Type
	t := reflect.TypeOf(inf)
	if t.Kind() == reflect.Ptr {
		tp = t.Elem()
	} else {
		tp = t
	}
	return tp
}

func getDirectStructFromInterface(inf interface{}) reflect.Value {
	var strct reflect.Value
	t := reflect.TypeOf(inf)
	if t.Kind() == reflect.Ptr {
		strct = reflect.ValueOf(inf).Elem()
	} else {
		strct = reflect.ValueOf(inf)
	}
	return strct
}

func getInterfacePointerSliceFromInterface(inf interface{}) []interface{} {
	v := getDirectStructFromInterface(inf)
	var objs []interface{}
	for i := 0; i < v.Len(); i++ {
		if v.Index(i).Kind() == reflect.Ptr {
			objs = append(objs, v.Index(i).Interface())
		} else {
			objs = append(objs, v.Index(i).Addr().Interface())
		}
	}
	return objs
}

func getInterfaceSliceFromInterface(inf interface{}) []interface{} {
	v := getDirectStructFromInterface(inf)
	var objs []interface{}
	if v.Kind() != reflect.Array && v.Kind() != reflect.Slice {
		return append(objs, inf)
	}
	for i := 0; i < v.Len(); i++ {
		if v.Index(i).Kind() == reflect.Ptr {
			objs = append(objs, v.Index(i).Elem().Interface())
		} else {
			objs = append(objs, v.Index(i).Interface())
		}
	}
	return objs
}
