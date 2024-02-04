package main

import (
	"database/sql/driver"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/anatol/smart.go"
	"github.com/jaypipes/ghw"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"io"
	"log"
	"os"
	"time"
)

const (
	OutputJson     = "json"
	OutputTable    = "table"
	OutputPostgres = "postgres"
)

type Attr smart.AtaSmartAttr

type PartitionLine struct {
	Uuid          string               `json:"uuid" db:"uuid"`
	Ts            time.Time            `json:"ts" db:"ts"`
	PartitionName string               `json:"partition_name" db:"partition_name"`
	Label         string               `json:"label" db:"label"`
	MountPath     string               `json:"mount_path" db:"mount_path"`
	SizeBytes     uint64               `json:"size_bytes" db:"size_bytes"`
	Attributes    []smart.AtaSmartAttr `json:"attributes" db:"attributes"`
}

type PartitionLineDb struct {
	Uuid          string       `db:"uuid"`
	Ts            time.Time    `db:"ts"`
	PartitionName string       `db:"partition_name"`
	Label         string       `db:"label"`
	MountPath     string       `db:"mount_path"`
	SizeBytes     uint64       `db:"size_bytes"`
	Attributes    driver.Value `db:"attributes"`
}

func (p *PartitionLine) partitionLineToDb() PartitionLineDb {
	attrs, _ := json.Marshal(p.Attributes)
	//attrStr := string(attrs)
	//attrStr = strings.Replace(attrStr, ":", "::", -1)
	//attrJson, _ := types.JSONText(attrs).Value()
	return PartitionLineDb{
		Uuid:          p.Uuid,
		Ts:            p.Ts,
		PartitionName: p.PartitionName,
		Label:         p.Label,
		MountPath:     p.MountPath,
		SizeBytes:     p.SizeBytes,
		Attributes:    string(attrs),
	}
}

type Config struct {
	Db         *DBConfig `json:"db,omitempty"`
	Attributes []uint8   `json:"attributes,omitempty"`
	Partitions []string  `json:"partitions"`
	OutputType string    `json:"output_type,omitempty"`
}

type DBConfig struct {
	Host               string `json:"host"`
	Port               int    `json:"port"`
	Username           string `json:"username"`
	Password           string `json:"password"`
	Schema             string `json:"schema"`
	Table              string `json:"table"`
	Initialize         bool   `json:"initialize,omitempty"`
	DataRetentionHours *int   `json:"data_retention_hours,omitempty"`
}

func saveToPostgresDB(record PartitionLine, conf DBConfig) {
	connStr := fmt.Sprintf("postgresql://%s:%s@%s:%d/postgres?sslmode=disable", conf.Username, conf.Password, conf.Host, conf.Port)
	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to create client: %v; %s", err, connStr)
	}

	if conf.Initialize {
		initTx := db.MustBegin()
		initTx.MustExec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", conf.Schema))
		initTx.MustExec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s ( uuid text, ts timestamp with time zone, partition_name text, label text, mount_path text, size_bytes numeric, attributes JSONB);", conf.Schema, conf.Table))
		if err != nil {
			log.Println(err)
			_ = initTx.Rollback()
		} else {
			fmt.Println("Committing Initialization")
			_ = initTx.Commit()
		}
	}

	towrite := record.partitionLineToDb()

	tx := db.MustBegin()
	res, err := tx.NamedExec(
		fmt.Sprintf(`INSERT INTO %s.%s (uuid, ts, partition_name, label, mount_path, size_bytes, attributes) VALUES (:uuid, :ts, :partition_name, :label, :mount_path, :size_bytes, :attributes);`,
			conf.Schema, conf.Table),
		&towrite)
	if err != nil {
		log.Println(err)
		_ = tx.Rollback()
	} else {
		rows, _ := res.RowsAffected()
		fmt.Printf("Commiting %d rows\n", rows)
		_ = tx.Commit()
	}

	if conf.DataRetentionHours != nil {
		if *conf.DataRetentionHours <= 0 {
			println("data retention days must be greater than zero if present, skipping")
		} else {
			cutoffTime := time.Now().Add(-1 * time.Hour * time.Duration(*conf.DataRetentionHours))
			txDel := db.MustBegin()
			delQuery := fmt.Sprintf("DELETE FROM %s.%s WHERE ts < '%s';", conf.Schema, conf.Table, cutoffTime.Format(time.RFC3339))
			res := txDel.MustExec(delQuery)

			if err != nil {
				log.Println(err)
				_ = txDel.Rollback()
			} else {
				rows, _ := res.RowsAffected()
				fmt.Printf("Deleted %d rows since %s by retention rule\n", rows, cutoffTime.Format(time.RFC3339))
				_ = txDel.Commit()
			}
		}
	}
}

// https://www.backblaze.com/blog/what-smart-stats-indicate-hard-drive-failures/
func main() {
	// Load Config
	confFiPath := flag.String("f", "conf.json", "Config File Path")
	flag.Parse()

	confFi, _ := os.Open(*confFiPath)

	var conf Config
	jsonBytes, _ := io.ReadAll(confFi)

	err := json.Unmarshal(jsonBytes, &conf)
	if err != nil {
		panic(fmt.Sprintf("Could not read Config File %s: %s\n", *confFiPath, err))
	} else {
		fmt.Printf("Using config file %s\n", *confFiPath)
	}

	// Set Defaults
	attrListToRead := []uint8{5, 187, 188, 197, 198}
	outputType := OutputJson

	if conf.Attributes != nil {
		attrListToRead = conf.Attributes
	}
	if conf.OutputType != "" {
		outputType = conf.OutputType
	}
	partitionList := make(map[string]bool)
	for _, partition := range conf.Partitions {
		partitionList[partition] = true

	}

	runTs := time.Now()

	// Get all Block Storage devices
	block, err := ghw.Block()
	if err != nil {
		panic(err)
	}

	// Check each disk partition
	for _, disk := range block.Disks {
		for _, p := range disk.Partitions {
			// Skip disks we don't care about
			devName := "/dev/" + p.Name
			if !partitionList[devName] {
				continue
			}

			dev, err := smart.Open(devName)
			if err != nil {
				// some devices (like dmcrypt) do not support SMART interface
				fmt.Printf("could not open disk %s, check sudo?: %s\n", devName, err)
				continue
			}

			defer dev.Close()

			switch sm := dev.(type) {
			case *smart.SataDevice:
				data, err := sm.ReadSMARTData()
				if err != nil {
					fmt.Printf("Could not read Sata Disk SMART data for %s: %s\n", devName, err)
					continue
				}

				attrResults := make([]smart.AtaSmartAttr, 0)

				for _, attrNum := range attrListToRead {
					attrResults = append(attrResults, data.Attrs[attrNum])
				}

				results := PartitionLine{
					Uuid:          p.UUID,
					Ts:            runTs,
					PartitionName: devName,
					Label:         p.FilesystemLabel,
					MountPath:     p.MountPoint,
					SizeBytes:     p.SizeBytes,
					Attributes:    attrResults,
				}

				if outputType == OutputJson {
					j, err := json.Marshal(results)
					if err != nil {
						fmt.Printf("json output error for %s: %s\n", devName, err)
						continue
					}
					fmt.Println(string(j))

				} else if outputType == OutputTable {
					println(devName)
					fmt.Printf("Current/Raw\n5 (%s): %d/%d\n187 (%s): %d/%d\n188 (%s): %d/%d\n197 (%s): %d/%d\n198 (%s): %d/%d\n",
						data.Attrs[5].Name, data.Attrs[5].Current, data.Attrs[5].ValueRaw,
						data.Attrs[187].Name, data.Attrs[187].Current, data.Attrs[187].ValueRaw,
						data.Attrs[188].Name, data.Attrs[188].Current, data.Attrs[188].ValueRaw,
						data.Attrs[197].Name, data.Attrs[197].Current, data.Attrs[197].ValueRaw,
						data.Attrs[198].Name, data.Attrs[198].Current, data.Attrs[198].ValueRaw)
					println()
				} else if outputType == OutputPostgres {
					if conf.Db == nil {
						println("No DB config, printing json")
						j, err := json.Marshal(results)
						if err != nil {
							fmt.Printf("json output error for %s: %s\n", devName, err)
							continue
						}
						fmt.Println(string(j))
					} else {
						saveToPostgresDB(results, *conf.Db)
					}
				}

			case *smart.ScsiDevice:
				_, _ = sm.Capacity()
			case *smart.NVMeDevice:
				_, _ = sm.ReadSMART()
			}
		}
	}
}
