//  Copyright (c) 2015 Rackspace
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
//  implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package andrewd

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/openstack/swift/go/hummingbird"
)

type BirdCatcher struct {
	oring      hummingbird.Ring
	logger     hummingbird.SysLogLike
	workingDir string
}

type ReconData struct {
	Device  string
	Mounted bool
	ip      string
	port    int
	dev     hummingbird.Device
}

var AccountName = ".admin"    //"AUTH_dfg"
var DbContainerName = "db"    //"AUTH_dfg"
var DbName = "birdcatcher.db" //"AUTH_dfg"

// TODO: just make these public in hbird somwhere
func map2Headers(m map[string]string) http.Header {
	if m == nil {
		return nil
	}
	headers := make(http.Header, len(m))
	for k, v := range m {
		headers.Set(k, v)
	}
	return headers
}

func headers2Map(headers http.Header) map[string]string {
	if headers == nil {
		return nil
	}
	m := make(map[string]string, len(headers))
	for k := range headers {
		m[k] = headers.Get(k)
	}
	return m
}

func (bc *BirdCatcher) doHealthCheck(ip string, port int) (ok bool) {
	return true

}

func (bc *BirdCatcher) reconGetUnmounted(ip string, port int,
	dataChan chan *ReconData,
	doneChan chan bool) (ipDown bool) {

	defer func() {
		doneChan <- true
	}()
	serverUrl := fmt.Sprintf("http://%s:%d/recon/unmounted", ip, port)

	fmt.Println(serverUrl)
	client := http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("GET", serverUrl, nil)
	if err != nil {
		bc.logger.Err(fmt.Sprintf("Could not create request to %s: %v",
			serverUrl, err))
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		bc.logger.Err(fmt.Sprintf("Could not do request to %s: %v",
			serverUrl, err))
		return true
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		bc.logger.Err(fmt.Sprintf("Could not read resp to %s: %v",
			serverUrl, err))
		return false
	}
	var serverReconData []*ReconData
	if err := json.Unmarshal(data, &serverReconData); err != nil {
		bc.logger.Err(fmt.Sprintf("Could not parse json from %s: %v",
			serverUrl, err))
		return false
	}

	for _, rData := range serverReconData {
		rData.ip = ip
		rData.port = port
		dataChan <- rData
	}
	return false

}

func (bc *BirdCatcher) GatherReconData() (devs []*ReconData, missingDevs []*hummingbird.Device) {

	type sKey struct {
		ip   string
		port int
	}
	allWeightedDevs := make(map[string]*hummingbird.Device)
	allServers := make(map[sKey]bool)

	for _, dev := range bc.oring.AllDevices() {
		if dev.Weight > 0 {
			allWeightedDevs[fmt.Sprintf(
				"%s:%d/%s", dev.Ip, dev.Port, dev.Device)] = &dev
			key := sKey{ip: dev.Ip, port: dev.Port}
			if _, ok := allServers[key]; !ok {
				allServers[key] = true
			}
		}
	}

	var unmountedReconData []*ReconData
	serverCount := 0
	dataChan := make(chan *ReconData)
	doneChan := make(chan bool)

	for key, _ := range allServers {
		go bc.reconGetUnmounted(key.ip, key.port, dataChan, doneChan)
		serverCount += 1
	}

	for serverCount > 0 {
		select {
		case rd := <-dataChan:
			unmountedReconData = append(unmountedReconData, rd)
			delete(allWeightedDevs,
				fmt.Sprintf("%s:%d/%s", rd.ip, rd.port, rd.Device))
		case <-doneChan:
			serverCount -= 1
		}
	}

	for _, wDev := range allWeightedDevs {
		missingDevs = append(missingDevs, wDev)
		/*
			errs = append(errs,
				errors.New(fmt.Sprintf("%s:%d/%s was not found in recon",
					wDev.Ip, wDev.Port, wDev.Device)))
		*/
	}
	return unmountedReconData, missingDevs
}

func (bc *BirdCatcher) getDb() (*sql.DB, error) {
	// this will later download the DB from out of the cluster but
	// for now will just load one up on /tmp/birdcatcher.db
	db, err := sql.Open("sqlite3", filepath.Join(bc.workingDir, DbName))
	if err != nil {
		fmt.Println("err on open: ", err)
		return nil, err
	}
	sqlCreate := "CREATE TABLE IF NOT EXISTS Device (" +
		"id INTEGER PRIMARY KEY, Ip VARCHAR(40), " +
		"Port INTEGER, Device VARCHAR(40), InRing INTEGER, " +
		"Weight FLOAT, Mounted INTEGER, Reachable INTEGER, " +
		"CreateDate DATETIME DEFAULT CURRENT_TIMESTAMP, " +
		"LastUpdate DATETIME DEFAULT CURRENT_TIMESTAMP);" +

		"CREATE TABLE IF NOT EXISTS DeviceLog (" +
		"DeviceId INTEGER, Mounted INTEGER, " +
		"CreateDate DATETIME DEFAULT CURRENT_TIMESTAMP, Notes VARCHAR(255), " +
		"FOREIGN KEY (DeviceId) REFERENCES Device(id));" +

		//		"CREATE TABLE IF NOT EXISTS Server (" +
		//		"id INTEGER PRIMARY KEY, Ip VARCHAR(40), Port INTEGER, " +
		//		"HealthCheck INTEGER, CreateDate DATETIME DEFAULT CURRENT_TIMESTAMP, " +
		//		"LastUpdate DATETIME DEFAULT CURRENT_TIMESTAMP);" +

		"CREATE TRIGGER IF NOT EXISTS DeviceLastUpdate " +
		"AFTER UPDATE ON Device FOR EACH ROW " +
		"BEGIN UPDATE Device SET LastUpdate = CURRENT_TIMESTAMP " +
		"WHERE id = OLD.id AND " +
		"(Mounted != OLD.Mounted OR Reachable != OLD.Reachable);END;" +

		//		"CREATE TRIGGER IF NOT EXISTS ServerLastUpdate " +
		//		"AFTER UPDATE ON Server FOR EACH ROW " +
		//		"BEGIN UPDATE Server SET LastUpdate = CURRENT_TIMESTAMP " +
		//		"WHERE id = OLD.id;END;" +

		"CREATE TRIGGER IF NOT EXISTS DeviceLogger " +
		"AFTER UPDATE ON Device FOR EACH ROW " +
		"BEGIN INSERT INTO DeviceLog (DeviceId, Mounted) " +
		"VALUES (OLD.id, OLD.Mounted);END;"
	//fmt.Println(sqlCreate)
	_, err = db.Exec(sqlCreate)
	if err != nil {
		fmt.Println("err on init Device: ", err)
		return nil, err
	}
	return db, nil
}

func (bc *BirdCatcher) updateDeviceData(allReconData []*ReconData) {

	db, err := bc.getDb()
	if err != nil {
		bc.logger.Err(fmt.Sprintf("Could not getDb: %v", err))
		return
	}
	for _, rd := range allReconData {
		fmt.Println("lalala", rd)
		/*
			newMounted := false
			if rd.Mounted {
				newMounted = true
			}
		*/
		row := db.QueryRow("SELECT Mounted FROM Device WHERE "+
			"Ip=? AND Port=? AND Device=?", rd.ip, rd.port, rd.Device)
		if row == nil {
			// do insert
			_, err = db.Exec("INSERT INTO Device "+
				"(RingId, Ip, Port, Device, Mounted) VALUES (?,?,?,?,?)",
				rd.dev.Id, rd.ip, rd.port, rd.Device, rd.Mounted)
			if err != nil {
				bc.logger.Err(fmt.Sprintf("Could not insert Device : %v", err))
			}
		} else {
			var dbMounted sql.NullBool
			if err := row.Scan(&dbMounted); err != nil {
				if !dbMounted.Valid || rd.Mounted != dbMounted.Bool {
					// do update
					_, err = db.Exec("UPDATE Device SET Mounted=? "+
						"WHERE Ip=? AND Port=? AND Device=?",
						rd.Mounted, rd.ip, rd.port, rd.Device)
					if err != nil {
						bc.logger.Err(
							fmt.Sprintf("Could not update Device: %v", err))
					}
				}
			}
		}
	}
	return
}

type serverTup struct {
	ip   string
	port int
}

func (bc *BirdCatcher) updateDbWithRing() error {
	db, err := bd.getDb()
	if err != nil {
		return err
	}
	allRingDevices := make(map[string]*hummingbird.Device)
	allWeightedServers := make(map[sKey]bool)
	for _, dev := range bc.oring.AllDevices() {
		allRingDevices[fmt.Sprintf("%s:%d/%s", dev.Ip, dev.Port, dev.Device)] = &dev

		if dev.Weight > 0 {
			key := serverTup{ip: dev.Ip, port: dev.Port}
			if _, ok := allWeightedServers[key]; !ok {
				allWeightedServers[key] = true
			}
		}
	}
	rows := db.Query("SELECT Ip, Port, Device, Weight FROM Device")
	var qryErrors []error
	for rows.Next() {
		var ip, device string
		var port int
		var weight float64

		if err := rows.Scan(&ip, &port, &device, &weight); err != nil {
			qryErrors = append(qryErrors, err)
		} else {
			ringWeight, found := allRingDevices[fmt.Sprintf("%s:%d/%s", ip, port, device)]
			if !found {
				//TODO- handle errors
				_, err = db.Exec("UPDATE Device SET InRing=false "+
					"WHERE Ip=? AND Port=? AND Device=?", ip, port, device)
			} else if ringWeight != weight {
				_, err = db.Exec("UPDATE Device SET Weight=? "+
					"WHERE Ip=? AND Port=? AND Device=?",
					ringWeight, ip, port, device)
			}
			delete(allRingDevices, fmt.Sprintf("%s:%d/%s", ip, port, device))
		}
	}
	for _, rDev := range allRingDevices {
		_, err = db.Exec("INSERT INTO Device "+
			"(Ip, Port, Device, InRing, Weight) VALUES"+
			"(?,?,?,?,?)",
			rDev.Ip, rDev.Port.rDev.Device, true, rDev.Weight)

	}
	//TODO - now query all inRing wieghted servers in DB and compare the mountedness.
	// just do a group by query for all the ip/ports then an up and set unmounted=1 for all the ones unmounted, and mounted=false for all the rest. the trigger will handle the updateing of LastUpdate
	rows := db.Query(
		"SELECT Ip, Port FROM Device WHERE Weight > 0 GROUP BY Ip, Port")
	for rows.Next() {
		var ip string
		var port int
		if err := rows.Scan(&ip, &port); err != nil {
			qryErrors = append(qryErrors, err)
		} else {

		}
		row := db.QueryRow("SELECT Weight FROM Device "+
			"WHERE Ip=? AND Port=? AND Device=?", dev.Ip, dev.Port, dev.Device)

		if row == nil {
			_, err = db.Exec("INSERT INTO Device "+
				"(Ip, Port, Device, Weight) VALUES (?,?,?,?)",
				dev.Ip, dev.Port, dev.Device, dev.Weight)
			if err != nil {
				bc.logger.Err(fmt.Sprintf("Could not insert Device : %v", err))
			}
		} else {
			var weight sql.NullFloat64
			if err := row.Scan(&weight); err != nil {
				if !weight.Valid || dev.Weight != weight.Float64 {
					_, err = db.Exec("UPDATE Device SET Weight=? "+
						"WHERE Ip=? AND Port=? AND Device=?",
						dev.Weight, dev.Ip, dev.Port, dev.Device)

				}

			}

		}

		if dev.Weight > 0 {
			allWeightedDevs[fmt.Sprintf(
				"%s:%d/%s", dev.Ip, dev.Port, dev.Device)] = &dev
			key := sKey{ip: dev.Ip, port: dev.Port}
			if _, ok := allServers[key]; !ok {
				allServers[key] = true
			}
		}
	}

}

func (bc *BirdCatcher) Run() error {
	// Get all devices in ring.
	// update database with new ring- set weights, set not NotInRing anymore, add new devices
	// for each server in DB call recon. update database as walking through.
	// if there's a change in mountedness / reachability, update row
	// query DB and pull any weighted devices that have been unmounted for > 1 week
	// remove those from ring

	// make dict {"ip:port/dev": -> weight}
	// Gather all unmounted / unreachable weighted devices from recon
	//
	// remove any devices that have been unmounted > a week
	// write a json object with results that can be used for monitoring
	unmountedReconDevs, missingDevs := bc.GatherReconData()

}

func GetBirdCatcher() (*BirdCatcher, error) {

	hashPathPrefix, hashPathSuffix, err := hummingbird.GetHashPrefixAndSuffix()
	if err != nil {
		fmt.Println("Unable to load hash path prefix and suffix:", err)
		return nil, err
	}
	objRing, err := hummingbird.GetRing(
		"object", hashPathPrefix, hashPathSuffix, 0)
	if err != nil {
		fmt.Println("Unable to load ring:", err)
		return nil, err
	}
	bc := &BirdCatcher{workingDir: "/tmp"}
	bc.getDb()
	bc.oring = objRing
	bc.logger = hummingbird.SetupLogger("LOG_LOCAL2", "birdcatcher", "")
	// fix at some point (add conf)
	return bc, nil

}
