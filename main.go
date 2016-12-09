package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/fatih/set"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"gopkg.in/alecthomas/kingpin.v2"
)

// db is the global database connection object
//var db *gorm.DB
type Shard struct {
	DB       *gorm.DB
	Families *set.Set
	status   bool
}

var databases []Shard

var (
	cli = kingpin.New("databalancer", "Micro-service for ingesting logs and balancing them across database tables")

	debug         = cli.Flag("debug", "Enable debug mode").Bool()
	dbUsername    = cli.Flag("mysql_username", "The MySQL user account username").Default("root").String() //dbuser
	dbPassword    = cli.Flag("mysql_password", "The MySQL user account password").Default("").String()     //dbpassword
	dbAddress     = cli.Flag("mysql_address", "The MySQL server address").Default("localhost:3306").String()
	dbName        = cli.Flag("mysql_databases", "The MySQL database to use").Default("databalancer,databalancer2").String()
	serverAddress = cli.Flag("server_address", "The address and port to serve the local HTTP server").Default(":8080").String()
)

// RawLog is an example struct which is used to store raw logs in the database
type RawLog struct {
	ID     uint
	Family string
	Log    string
}

// Any tables in this array will automatically be dropped and re-created every
// time the binary starts. This may become undesired behavior eventually.
var databaseTables = [...]interface{}{
	&RawLog{},
}

// IngestLogBody is the format of the JSON required in the body of a request to
// the IngestLog handler
type IngestLogBody struct {
	Family string                   `json:"family" binding:"required"`
	Schema map[string]string        `json:"schema" binding:"required"`
	Logs   []map[string]interface{} `json:"logs" binding:"required"`
}

type QueryBody struct {
	SQL string `json:"sql_query" binding:"required"`
}

func Random() int {
	return rand.Intn(len(databases))
}

// IngestLog is an HTTP handler which ingests logs from other micro-services
func IngestLog(c *gin.Context) {
	var body IngestLogBody

	err := c.BindJSON(&body)

	if err != nil {
		logrus.WithError(err).Errorf("The request did not contain a correctly formatted JSON body")

		return
	}

	logrus.Debugf("Received logs for the %s log family", body.Family)

	//get existing family names
	var sharder Shard
	for _, shard := range databases {
		if shard.DB.HasTable(body.Family) {
			shard.Families.Add(body.Family)
			sharder = shard
		}
	}

	if !sharder.status {
		sharder = databases[Random()]
		createString := "create table " + body.Family + " ( "
		for column, columnType := range body.Schema {
			logrus.Debugf("Log values for the field %s of the %s log will be of type %s", column, body.Family, columnType)
			switch columnType {
			case "string":
				createString = createString + " " + column + " varchar(255),"
			case "int":
				createString = createString + " " + column + " INT,"
			}
		}
		createString = createString + ")"
		sharder.DB.Exec(strings.Replace(createString, ",)", ")", 1))
		sharder.Families.Add(body.Family)
	}

	for _, logEvent := range body.Logs {
		logrus.Debugf("Handling a new log event for the %s log family", body.Family)

		for field, value := range logEvent {
			columnType, ok := body.Schema[field]

			if !ok {
				c.JSON(http.StatusBadRequest, map[string]string{
					"message": fmt.Sprintf(
						"Data type for the field %s was not specified in the %s schema map",
						field,
						body.Family,
					),
				})

				return
			}

			switch columnType {
			case "string":
				logrus.Debugf("The value of the %s field in the %s log event is %s", field, body.Family, value.(string))
			case "int":
				logrus.Debugf("The value of the %s field in the %s log event is %d", field, body.Family, int(value.(float64)))
			default:

				c.JSON(http.StatusBadRequest, map[string]string{
					"message": fmt.Sprintf("Unsupported data type in %s log for the field %s: %s", body.Family, field, columnType),
				})

				return
			}
		}

		// Marshal the log event back into JSON to store it in the database
		rawLogContent, err := json.Marshal(logEvent)

		if err != nil {
			logrus.WithError(err).Errorln("Could not marshal the log event into JSON")

			c.JSON(http.StatusInternalServerError, map[string]string{
				"message": "JSON error",
			})

			return
		}

		rawLog := RawLog{
			Family: body.Family,
			Log:    string(rawLogContent),
		}

		err = sharder.DB.Create(&rawLog).Error
		if err != nil {
			logrus.WithError(err).Errorln("Cold not store the log event in the database")

			c.JSON(http.StatusInternalServerError, map[string]string{
				"message": "Database error",
			})

			return
		}

	}
	//Hacky mess but it works
	insertList := make([]string, len(body.Logs))
	for _, event := range body.Logs {
		insertColumn := "INSERT INTO " + body.Family + " ( "
		insertValues := "Values ("
		for field, value := range event {
			insertColumn = insertColumn + field + ","
			switch value.(type) {
			case string:
				insertValues = insertValues + `"` + value.(string) + `",`
			case float64:
				insertValues = insertValues + strconv.Itoa(int(value.(float64))) + ","
			}
		}
		insertValues = insertValues + ")"
		insertColumn = insertColumn + ")"
		insertList = append(insertList, (strings.Replace(insertColumn, ",)", ")", -1) + strings.Replace(insertValues, ",)", ")", -1)))
	}
	for _, val := range insertList {
		sharder.DB.Exec(val)
	}

	c.JSON(http.StatusOK, map[string]string{
		"message": "OK",
	})
}
func QueryMagic(c *gin.Context) {
	var body QueryBody

	err := c.BindJSON(&body)
	if err != nil {
		logrus.WithError(err).Errorf("The request did not contain a correctly formatted JSON body")
		return
	}
	var sharder Shard
	for _, shard := range databases {
		rows, _ := shard.DB.Raw("show tables").Rows()
		for rows.Next() {
			val := ""
			rows.Scan(&val)
			shard.Families.Add(val)
		}
	}
	fmt.Println(databases[0].Families.List())
	fmt.Println(databases[1].Families.List())
LOOP:
	for _, shard := range databases {
		for _, x := range shard.Families.List() {
			if strings.Contains(body.SQL, x.(string)) {
				sharder = shard
				fmt.Println(shard)
				break LOOP
			}
		}

	}
	fmt.Println(sharder.status)

	if sharder.status {

		rows, err := sharder.DB.Raw(body.SQL).Rows()
		if err != nil {
			logrus.Warning(err)
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		rawResult := make([][]byte, len(cols))
		result := make([]string, len(cols))
		dest := make([]interface{}, len(cols))
		for i, _ := range rawResult {
			dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
		}
		var something []string
		for rows.Next() {
			err = rows.Scan(dest...)
			if err != nil {
				fmt.Println("Failed to scan row", err)
				return
			}

			for i, raw := range rawResult {
				if raw == nil {
					result[i] = "\\N"
				} else {
					result[i] = string(raw)
				}
			}
			something = append(something, ("{" + strings.Join(result, ",") + "}"))
			fmt.Println(result)
		}
		c.JSON(http.StatusAccepted, gin.H{
			"result": something,
		})
	}
}

func main() {
	// Key variables are set as command-line flags
	_, err := cli.Parse(os.Args[1:])

	if err != nil {
		logrus.WithError(err).Fatal("Error parsing command-line arguments")
	}

	if *debug {
		// Enable debug logging
		logrus.SetLevel(logrus.DebugLevel)
	}
	databasesNames := strings.Split(fmt.Sprintf("%s", *dbName), ",")

	// Using data from command-line parameters, we create a MySQL connection
	// string
	for _, val := range databasesNames {
		connectionString := fmt.Sprintf(
			"%s:%s@(%s)/%s?charset=utf8&parseTime=True&loc=Local",
			*dbUsername,
			*dbPassword,
			*dbAddress,
			val,
		)
		shard := Shard{}
		db, err := gorm.Open("mysql", connectionString)
		if err != nil {
			logrus.WithError(err).Fatal("Could not establish a connection to the database")
		}
		shard.DB = db
		shard.Families = set.New()
		shard.status = true
		logrus.Infof("Connected to MySQL as %s at %s", *dbUsername, *dbAddress)
		databases = append(databases, shard)
	}

	for _, shard := range databases {
		for _, table := range databaseTables {
			shard.DB.DropTableIfExists(table)
			shard.DB.CreateTable(table)
		}
	}

	logrus.Infof("Starting HTTP server on %s", *serverAddress)

	// Now that we have performed all required flag parsing and state
	// initialization, we create and launch our HTTP web server for our
	// micro-service
	r := gin.New()

	r.PUT("/api/log", IngestLog)
	r.PUT("/api/query", QueryMagic)

	r.Run(*serverAddress)
}
