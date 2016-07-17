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
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
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

func (bc *BirdCatcher) GatherReconData() (devs []*ReconData, errs []error) {

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

	var allReconData []*ReconData
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
			allReconData = append(allReconData, rd)
			delete(allWeightedDevs,
				fmt.Sprintf("%s:%d/%s", rd.ip, rd.port, rd.Device))
		case <-doneChan:
			serverCount -= 1
		}
	}

	for _, wDev := range allWeightedDevs {
		errs = append(errs,
			errors.New(fmt.Sprintf("%s:%d/%s was not found in recon",
				wDev.Ip, wDev.Port, wDev.Device)))
	}
	return allReconData, errs
}

func (bc *BirdCatcher) getDb() (*sql.DB, error) {
	// this will later download the DB from out of the cluster but
	// for now will just load one up on /tmp/birdcatcher.db
	db, err := sql.Open("sqlite3", "/tmp/bc.db")
	if err != nil {
		fmt.Println("err on open: ", err)
		return nil, err
	}
	sqlCreate := "CREATE TABLE IF NOT EXISTS Device (" +
		"id INTEGER PRIMARY KEY, RingId INTEGER, " +
		"Ip VARCHAR(40), Port INTEGER, Device VARCHAR(40), " +
		"Mounted INTEGER, CreateDate DATETIME DEFAULT CURRENT_TIMESTAMP, " +
		"LastUpdate DATETIME DEFAULT CURRENT_TIMESTAMP);" +

		"CREATE TABLE IF NOT EXISTS DeviceLog (" +
		"DeviceId INTEGER, Mounted INTEGER, " +
		"CreateDate DATETIME DEFAULT CURRENT_TIMESTAMP, Notes VARCHAR(255), " +
		"FOREIGN KEY (DeviceId) REFERENCES Device(id));" +

		"CREATE TABLE IF NOT EXISTS Server (" +
		"id INTEGER PRIMARY KEY, Ip VARCHAR(40), Port INTEGER, " +
		"HealthCheck INTEGER, CreateDate DATETIME DEFAULT CURRENT_TIMESTAMP, " +
		"LastUpdate DATETIME DEFAULT CURRENT_TIMESTAMP);" +

		"CREATE TRIGGER IF NOT EXISTS DeviceLastUpdate " +
		"AFTER UPDATE ON Device FOR EACH ROW " +
		"BEGIN UPDATE Device SET LastUpdate = CURRENT_TIMESTAMP " +
		"WHERE id = OLD.id;END;" +

		"CREATE TRIGGER IF NOT EXISTS ServerLastUpdate " +
		"AFTER UPDATE ON Server FOR EACH ROW " +
		"BEGIN UPDATE Server SET LastUpdate = CURRENT_TIMESTAMP " +
		"WHERE id = OLD.id;END;"
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
