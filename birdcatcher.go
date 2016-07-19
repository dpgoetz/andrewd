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
	doneServersChan chan ipPort) {

	ipPortCalling := ipPort{ip: ip, port: port, up: false}
	defer func() {
		doneServersChan <- ipPortCalling
	}()

	serverUrl := fmt.Sprintf("http://%s:%d/recon/unmounted", ip, port)

	fmt.Println(serverUrl)
	client := http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("GET", serverUrl, nil)
	if err != nil {
		bc.logger.Err(fmt.Sprintf("Could not create request to %s: %v",
			serverUrl, err))
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		bc.logger.Err(fmt.Sprintf("Could not do request to %s: %v",
			serverUrl, err))
		return
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		bc.logger.Err(fmt.Sprintf("Could not read resp to %s: %v",
			serverUrl, err))
		return
	}
	var serverReconData []*ReconData
	if err := json.Unmarshal(data, &serverReconData); err != nil {
		bc.logger.Err(fmt.Sprintf("Could not parse json from %s: %v",
			serverUrl, err))
		return
	}

	ipPortCalling.up = true
	for _, rData := range serverReconData {
		rData.ip = ip
		rData.port = port
		dataChan <- rData
	}
}

func (bc *BirdCatcher) deviceId(ip string, port int, device string) string {
	return fmt.Sprintf("%s:%d/%s", ip, port, device)
}

func (bc *BirdCatcher) serverId(ip string, port int) string {
	return fmt.Sprintf("%s:%d", ip, port)
}

func (bc *BirdCatcher) gatherReconData(servers []ipPort) (devs []*ReconData, downServers map[string]ipPort) {

	serverCount := 0
	dataChan := make(chan *ReconData)
	doneServersChan := make(chan ipPort)
	downServers = make(map[string]ipPort)

	for _, s := range servers {
		go bc.reconGetUnmounted(s.ip, s.port, dataChan, doneServersChan)
		serverCount += 1
	}

	for serverCount > 0 {
		select {
		case rd := <-dataChan:
			devs = append(devs, rd)
		case ipp := <-doneServersChan:
			if ipp.up != true {
				fmt.Println("poooooo: ", downServers)
				downServers[bc.serverId(ipp.ip, ipp.port)] = ipp
			}
			serverCount -= 1
		}
	}

	return devs, downServers
}

func (bc *BirdCatcher) getDb() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", filepath.Join(bc.workingDir, DbName))
	if err != nil {
		fmt.Println("err on open: ", err)
		return nil, err
	}
	tx, err := db.Begin()
	if err != nil {
		fmt.Println("err on begin: ", err)
		return nil, err
	}
	sqlCreate := "CREATE TABLE IF NOT EXISTS Device (" +
		"id INTEGER PRIMARY KEY, Ip VARCHAR(40) NOT NULL, " +
		"Port INTEGER NOT NULL, Device VARCHAR(40) NOT NULL, InRing INTEGER NOT NULL, " +
		"Weight FLOAT NOT NULL, Mounted INTEGER NOT NULL, Reachable INTEGER NOT NULL, " +
		"CreateDate DATETIME DEFAULT CURRENT_TIMESTAMP, " +
		"LastUpdate DATETIME DEFAULT CURRENT_TIMESTAMP);" +

		"CREATE TABLE IF NOT EXISTS DeviceLog (" +
		"DeviceId INTEGER, Mounted INTEGER, Reachable INTEGER " +
		"CreateDate DATETIME DEFAULT CURRENT_TIMESTAMP, Notes VARCHAR(255), " +
		"FOREIGN KEY (DeviceId) REFERENCES Device(id));" +

		"CREATE TRIGGER IF NOT EXISTS DeviceLastUpdate " +
		"AFTER UPDATE ON Device FOR EACH ROW " +
		"BEGIN UPDATE Device SET LastUpdate = CURRENT_TIMESTAMP " +
		"WHERE id = OLD.id AND " +
		"(Mounted != OLD.Mounted OR Reachable != OLD.Reachable);END;" +

		"CREATE TRIGGER IF NOT EXISTS DeviceLogger " +
		"AFTER UPDATE ON Device FOR EACH ROW " +
		"BEGIN INSERT INTO DeviceLog (DeviceId, Mounted, Reachable) " +
		"VALUES (OLD.id, OLD.Mounted, OLD.Reachable);END;"
	//fmt.Println(sqlCreate)
	_, err = db.Exec(sqlCreate)
	if err != nil {
		fmt.Println("err on init Device: ", err)
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		fmt.Println("err on commit: ", err)
		return nil, err
	}
	return db, nil
}

type ipPort struct {
	ip   string
	port int
	up   bool
}

func (bc *BirdCatcher) getRingData() (map[string]hummingbird.Device, []ipPort) {

	allRingDevices := make(map[string]hummingbird.Device)
	var allWeightedServers []ipPort
	weightedServers := make(map[string]bool)
	for _, dev := range bc.oring.AllDevices() {
		allRingDevices[bc.deviceId(dev.Ip, dev.Port, dev.Device)] = dev

		if dev.Weight > 0 {
			if _, ok := weightedServers[bc.serverId(dev.Ip, dev.Port)]; !ok {
				allWeightedServers =
					append(allWeightedServers, ipPort{ip: dev.Ip, port: dev.Port})
				weightedServers[bc.serverId(dev.Ip, dev.Port)] = true
			}
		}
	}
	return allRingDevices, allWeightedServers

}

func (bc *BirdCatcher) updateDb() error {
	db, err := bc.getDb()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	allRingDevices, allWeightedServers := bc.getRingData()
	unmountedDevices := make(map[string]bool)

	reconDevices, downServers := bc.gatherReconData(allWeightedServers)
	fmt.Println("vvvvvvvvv: ", reconDevices[0].Mounted)
	for _, rData := range reconDevices {
		if !rData.Mounted {
			unmountedDevices[bc.deviceId(rData.ip, rData.port, rData.Device)] = rData.Mounted
		}
	}
	rows, _ := tx.Query("SELECT Ip, Port, Device, Weight, Mounted FROM Device")
	var qryErrors []error
	for rows.Next() {
		var ip, device string
		var port int
		var mounted bool
		var weight float64

		if err := rows.Scan(&ip, &port, &device, &weight, &mounted); err != nil {
			fmt.Println("z11111111")
			qryErrors = append(qryErrors, err)
		} else {
			fmt.Println("z2222222")

			dKey := bc.deviceId(ip, port, device)
			rDev, inRing := allRingDevices[dKey]
			_, inUnmounted := unmountedDevices[dKey]
			_, notReachable := downServers[bc.serverId(ip, port)]
			if !inRing {
				//TODO- handle errors
				fmt.Println("z33333333")
				_, err = tx.Exec("UPDATE Device SET InRing=0 "+
					"WHERE Ip=? AND Port=? AND Device=?", ip, port, device)
			} else {
				if !notReachable {
					fmt.Println("z444444: ", inUnmounted)
					if rDev.Weight != weight || mounted == inUnmounted {
						_, err = tx.Exec("UPDATE Device SET "+
							"Weight=?, Mounted=? "+
							"WHERE Ip=? AND Port=? AND Device=?",
							rDev.Weight, !inUnmounted, ip, port, device)
						fmt.Println("frfrf: ", err)
					}
				}
			}
			delete(allRingDevices, dKey)
		}
	}
	rows.Close()
	for _, ipp := range downServers {
		_, err = tx.Exec("UPDATE Device SET Reachable=0 "+
			"WHERE Ip=? AND Port=?", ipp.ip, ipp.port)
	}
	for _, rDev := range allRingDevices {
		dKey := bc.deviceId(rDev.Ip, rDev.Port, rDev.Device)
		_, isUnmounted := unmountedDevices[dKey]
		_, notReachable := downServers[bc.serverId(rDev.Ip, rDev.Port)]
		_, err = tx.Exec("INSERT INTO Device "+
			"(Ip, Port, Device, InRing, Weight, Mounted, Reachable) VALUES"+
			"(?,?,?,?,?,?,?)",
			rDev.Ip, rDev.Port, rDev.Device,
			true, rDev.Weight, !isUnmounted, !notReachable)
		fmt.Println("inserting: ", rDev.Device, err, !isUnmounted)
	}
	if err = tx.Commit(); err != nil {
		fmt.Println("$$$$$$$$$$$$$$$$$$$$$$$$")

		return err
	}
	return nil
}

/*
func (bc *BirdCatcher) updateRing() error {
	db, err := bc.getDb()
	if err != nil {
		return err
	}
	rows, _ := db.Query("SELECT Ip, Port, Device, Weight, Mounted FROM Device")
	var qryErrors []error
	for rows.Next() {
		var ip, device string // think i have to do that sql.null thing here
		var port, mounted int
		var weight float64

		if err := rows.Scan(&ip, &port, &device, &weight); err != nil {
			qryErrors = append(qryErrors, err)
		} else {
		}
	}
	return nil
}
*/

func (bc *BirdCatcher) Run() {
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
	bc.updateDb()
	//bc.updateRing()

}

func GetBirdCatcher(serverconf hummingbird.Config) (*BirdCatcher, error) {

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
	workingDir, ok := serverconf.Get("andrewd", "working_dir")
	if !ok {
		panic("Invalid Config")
	}
	bc := &BirdCatcher{workingDir: workingDir}
	//bc.getDb()
	bc.oring = objRing
	bc.logger = hummingbird.SetupLogger(serverconf.GetDefault(
		"andrewd", "log_facility", "LOG_LOCAL0"), "birdcatcher", "")
	return bc, nil

}
