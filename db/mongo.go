// This package acts as an extremely simple object document mapper for mongodb and manages basic functionality of mongodb.
package db

import (
	"context"
	"net/url"
	"os"
	"reflect"
	"strings"
	"time"

	// "github.com/kamva/mgm/v3"
	// "git.sys-tem.org/caos/db4bigdata/internal/model"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// contains the mongodb's URL, UserName and Password
type MongoConfig struct {
	URL      url.URL
	UserName string
	Password string
}

// knows the current connection to the mongodb, database and the current context.
type Database struct {
	conn    *mongo.Client
	db      *mongo.Database
	context *context.Context
}

// holds reflection of a struct as list of abstractStructField
type abstractStructFieldSet struct {
	fields []abstractStructField
}

// has key and reflect.Value of a structfield
type abstractStructField struct {
	key   string
	value reflect.Value
	tp    reflect.StructField
}

// drops the current db and starts over from zero - use with caution
func Initialize(mongo *Database) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := mongo.db.Drop(ctx)

	if err == nil {
		return true
	}
	return false
}

// connects to the configured mongodb and returns the connection
func ConnectMongo() (*Database, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mongourl := os.Getenv("MONGO_CONNECTION")
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongourl))
	logrus.Println(os.Getenv("MONGO_DB"))
	db := &Database{client, client.Database(os.Getenv("MONGO_DB")), &ctx}
	return db, err
}

// drops a given doc
func (mongo *Database) Drop(collectionName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := mongo.db.Collection(collectionName).Drop(ctx); err != nil {
		logrus.Errorln(err)
		return err
	}
	return nil
}

// saves a new doc in mongo - decides on its own if it has to do an upsert or insert, by the given filter
func (mongo *Database) Save(obj interface{}, filter string) error {
	t := getDirectTypeFromInterface(obj)
	coll := mongo.db.Collection(getNestedElemName(t))
	logrus.Println(getNestedElemName(t))
	objs := getInterfaceSliceFromInterface(obj)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opts := options.Update().SetUpsert(true)

	if filter != "" {
		for _, o := range objs {
			// filterval := reflect.
			update := bson.D{{"$set", o}}
			filterval := getStructFieldByMongoFilterName(o, filter)
			filterqry := bson.D{{filter, filterval}}
			res, err := coll.UpdateOne(ctx, filterqry, update, opts)
			if err != nil {
				logrus.Errorln("Error when inserting objects: ", err)
				return err
			}
			logrus.Printf("upserted %d documents for Collection %s, updated %d documents", res.UpsertedCount, getNestedElemName(t), res.ModifiedCount)
		}
	} else {
		res, err := coll.InsertMany(ctx, objs)
		if err != nil {
			logrus.Errorln("Error when inserting objects: ", err)
			return err
		}
		logrus.Printf("Inserted %d documents for Collection %s", len(res.InsertedIDs), getNestedElemName(t))
	}

	return nil
}

// deletes a given doc
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

// returns the first found doc by filter-query on a target struct
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

// closes the database connection
func (mongo *Database) Close() error {
	err := mongo.conn.Disconnect(*mongo.context)
	return err
}

// gets the name of a filter by the given struct field
func getStructFieldByMongoFilterName(inf interface{}, name string) interface{} {
	strct := getDirectStructFromInterface(inf)
	capname := strings.Title(name)
	field := strct.FieldByName(capname)
	return field.Interface()
}

// gets all fields of the given struct as a slice
func resolveStructFields(inf interface{}) []reflect.StructField {
	strct := getDirectTypeFromInterface(inf)
	fields := []reflect.StructField{}
	for i := 0; i < strct.NumField(); i++ {
		fields = append(fields, strct.Field(i))
	}
	return fields
}

// returns an abstractStructFieldSet by a given type
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

// gets all nested element names as string
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

// returns a direct handler if a pointer or other indirect was given - if it is already a direct handler, the same is returned
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

// gets a direct handler if the given interface is a pointer
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

// returns a slice of pointers to interfaces if the given type was an array or slice
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

// returns a slice of interfaces if the given type was an array or slice
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
