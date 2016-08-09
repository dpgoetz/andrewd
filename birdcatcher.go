//  Copyright (c) 2016 Rackspace
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
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/openstack/swift/go/hummingbird"
)

type BirdCatcher struct {
	oring           hummingbird.Ring
	logger          hummingbird.SysLogLike
	workingDir      string
	maxAge          time.Duration
	maxWeightChange float64
	ringBuilder     string
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

	//fmt.Println(serverUrl)
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

		"CREATE TABLE IF NOT EXISTS RingActions (" +
		"id INTEGER PRIMARY KEY, Ip VARCHAR(40) NOT NULL, " +
		"Port INTEGER NOT NULL, Device VARCHAR(40) NOT NULL, " +
		"Action VARCHAR(20) NOT NULL, Success INTEGER NOT NULL, " +
		"CreateDate DATETIME DEFAULT CURRENT_TIMESTAMP);" +

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
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (bc *BirdCatcher) getDevicesToUnmount() ([]hummingbird.Device, err) {

}

func (bc *BirdCatcher) updateRing() (output []string, cmdErrors []error) {
	allRingDevices, _ := bc.getRingData()
	totalWeight := float64(0)

	for _, dev := range allRingDevices {
		totalWeight += dev.Weight
	}

	db, err := bc.getDb()
	if err != nil {
		cmdErrors = append(cmdErrors, err)
		return nil, cmdErrors
	}
	now := time.Now()
	rows, err := db.Query("SELECT Ip, Port, Device, Weight, Mounted, Reachable FROM Device WHERE (Mounted=0 OR Reachable=0) AND LastUpdate < ? ORDER BY LastUpdate", now.Add(-bc.maxAge))
	var qryErrors []error
	var badDevices []hummingbird.Device

	weightToUnmount := float64(0)
	for rows.Next() {
		var ip, device string // think i have to do that sql.null thing here
		var port int
		var weight float64
		var mounted, reachable bool

		if err := rows.Scan(&ip, &port, &device, &weight, &mounted, &reachable); err != nil {
			qryErrors = append(qryErrors, err)
		} else {
			weightToUnmount += weight
			if weightToUnmount < totalWeight*bc.maxWeightChange {
				badDevices = append(badDevices, hummingbird.Device{Ip: ip, Port: port, Device: device, Weight: weight})
			}
		}
	}
	rows.Close()

	tx, err := db.Begin()
	if err != nil {
		cmdErrors = append(cmdErrors, err)
		return nil, cmdErrors
	}

	success := true
	for _, dev := range badDevices {
		devKey := bc.deviceId(dev.Ip, dev.Port, dev.Device)
		cmd := exec.Command(
			"swift-ring-builder", bc.ringBuilder, "set_weight", devKey, "0")
		var out bytes.Buffer
		cmd.Stdout = &out
		if err := cmd.Run(); err != nil {
			cmdErrors = append(cmdErrors, err)
			success = false
		} else {
			output = append(output, out.String())
		}
	}
	cmd := exec.Command("swift-ring-builder", bc.ringBuilder, "rebalance")
	var out bytes.Buffer
	cmd.Stdout = &out
	success := true
	if err := cmd.Run(); err != nil {
		cmdErrors = append(cmdErrors, err)
		return nil, cmdErrors
	} else {
		output = append(output, out.String())
	}

	for _, dev := range badDevices {
		_, err = tx.Exec("INSERT INTO RingActions "+
			"(Ip, Port, Device, Action, Success) VALUES "+
			"(?,?,?,?,?)", dev.Ip, dev.Port, dev.Device, "ZEROED", success)
	}
	if err = tx.Commit(); err != nil {
		cmdErrors = append(cmdErrors, err)
		return nil, cmdErrors
	}

	// update database
	return output, cmdErrors
}

func (bc *BirdCatcher) Run() {
	bc.updateDb()
	bc.updateRing()
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
	maxAge := time.Duration(serverconf.GetInt("andrewd", "max_age_sec", int64(hummingbird.ONE_WEEK*time.Second)))
	maxWeightChange := serverconf.GetFloat("andrewd", "max_weight_change", 0.005)
	ringBuilder := serverconf.GetDefault("andrewd", "ring_builder", "/etc/swift/object.builder")

	bc := &BirdCatcher{workingDir: workingDir,
		maxAge:          maxAge,
		maxWeightChange: maxWeightChange,
		ringBuilder:     ringBuilder}
	//bc.getDb()
	bc.oring = objRing
	bc.logger = hummingbird.SetupLogger(serverconf.GetDefault(
		"andrewd", "log_facility", "LOG_LOCAL0"), "birdcatcher", "")
	return bc, nil

}
