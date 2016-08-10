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

const ONE_DAY = 86400

type BirdCatcher struct {
	oring           hummingbird.Ring
	logger          hummingbird.SysLogLike
	workingDir      string
	reportDir       string
	maxAge          time.Duration
	maxWeightChange float64
	ringBuilder     string
	durBetweenRun   time.Duration
	db              *sql.DB
}

type ReconData struct {
	Device  string
	Mounted bool
	ip      string
	port    int
	dev     hummingbird.Device
}

var DbName = "birdcatcher.db" //"AUTH_dfg"
var ReportName = "unmounted_report.json"

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
	// replace this with hbird FullName when it ghets merged
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
	// TODO think i need to get this to return a transaction
	// this is not thread safe but i don't think i care
	if bc.db != nil {
		if err := bc.db.Ping(); err == nil {
			return bc.db, nil
		}
		bc.db.Close()
	}
	db, err := sql.Open("sqlite3", filepath.Join(bc.workingDir, DbName))
	if err != nil {
		return nil, err
	}
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	sqlCreate := "CREATE TABLE IF NOT EXISTS Device (" +
		// DFG TODO: need to add Region and Zone to this (maybe not...)
		"id INTEGER PRIMARY KEY, Ip VARCHAR(40) NOT NULL, " +
		"Port INTEGER NOT NULL, Device VARCHAR(40) NOT NULL, InRing INTEGER NOT NULL, " +
		"Weight FLOAT NOT NULL, Mounted INTEGER NOT NULL, Reachable INTEGER NOT NULL, " +
		"CreateDate DATETIME DEFAULT CURRENT_TIMESTAMP, " +
		"LastUpdate DATETIME DEFAULT CURRENT_TIMESTAMP);" +

		"CREATE TABLE IF NOT EXISTS DeviceLog (" +
		"DeviceId INTEGER, Mounted INTEGER, Reachable INTEGER " +
		"CreateDate DATETIME DEFAULT CURRENT_TIMESTAMP, Notes VARCHAR(255), " +
		"FOREIGN KEY (DeviceId) REFERENCES Device(id));" +

		"CREATE TABLE IF NOT EXISTS RingAction (" +
		"id INTEGER PRIMARY KEY, Ip VARCHAR(40) NOT NULL, " +
		"Port INTEGER NOT NULL, Device VARCHAR(40) NOT NULL, " +
		"Action VARCHAR(20) NOT NULL, CreateDate DATETIME NOT NULL);" +

		"CREATE TRIGGER IF NOT EXISTS DeviceLastUpdate " +
		"AFTER UPDATE ON Device FOR EACH ROW " +
		"BEGIN UPDATE Device SET LastUpdate = CURRENT_TIMESTAMP " +
		"WHERE id = OLD.id AND " +
		"(Mounted != OLD.Mounted OR Reachable != OLD.Reachable);END;" +

		"CREATE TRIGGER IF NOT EXISTS DeviceLogger " +
		"AFTER UPDATE ON Device FOR EACH ROW " +
		"BEGIN INSERT INTO DeviceLog (DeviceId, Mounted, Reachable) " +
		"VALUES (OLD.id, OLD.Mounted, OLD.Reachable);END;"
	_, err = db.Exec(sqlCreate)
	if err != nil {
		fmt.Println("err on init Device: ", err)
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		fmt.Println("err on commit: ", err)
		return nil, err
	}
	bc.db = db
	return bc.db, nil
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

func (bc *BirdCatcher) needRingUpdate() bool {

	db, err := bc.getDb()
	if err != nil {
		bc.logger.Err(fmt.Sprintf("ERROR: getDb for needRingUpdate: %v", err))
		return false
	}
	//defer db.Close()
	rows, err := db.Query("SELECT MAX(CreateDate) FROM RingAction")
	defer rows.Close()
	if err != nil {
		bc.logger.Err(fmt.Sprintf("ERROR: SELECT for needRingUpdate: %v", err))
		return false
	}
	var createDate time.Time
	rows.Next()
	if err := rows.Scan(&createDate); err != nil {
		// TODO: what happens on first run?
		bc.logger.Err(fmt.Sprintf("ERROR: parse for needRingUpdate: %v", err))
		return false
	}
	return time.Since(createDate) > bc.durBetweenRun

}

func (bc *BirdCatcher) updateDb() error {
	db, err := bc.getDb()
	if err != nil {
		return err
	}
	//defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	allRingDevices, allWeightedServers := bc.getRingData()
	unmountedDevices := make(map[string]bool)

	reconDevices, downServers := bc.gatherReconData(allWeightedServers)
	for _, rData := range reconDevices {
		if !rData.Mounted {
			unmountedDevices[bc.deviceId(rData.ip, rData.port, rData.Device)] = rData.Mounted
		}
	}
	rows, _ := tx.Query("SELECT Ip, Port, Device, Weight, Mounted FROM Device")
	defer rows.Close()
	var qryErrors []error
	for rows.Next() {
		var ip, device string
		var port int
		var mounted bool
		var weight float64

		if err := rows.Scan(&ip, &port, &device, &weight, &mounted); err != nil {
			qryErrors = append(qryErrors, err)
		} else {

			dKey := bc.deviceId(ip, port, device)
			rDev, inRing := allRingDevices[dKey]
			_, inUnmounted := unmountedDevices[dKey]
			_, notReachable := downServers[bc.serverId(ip, port)]
			if !inRing {
				//TODO- handle errors
				_, err = tx.Exec("UPDATE Device SET InRing=0 "+
					"WHERE Ip=? AND Port=? AND Device=?", ip, port, device)
			} else {
				if !notReachable {
					if rDev.Weight != weight || mounted == inUnmounted {
						_, err = tx.Exec("UPDATE Device SET "+
							"Weight=?, Mounted=? "+
							"WHERE Ip=? AND Port=? AND Device=?",
							rDev.Weight, !inUnmounted, ip, port, device)
					}
				}
			}
			delete(allRingDevices, dKey)
		}
	}
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

func (bc *BirdCatcher) getDevicesToUnmount() (badDevices []hummingbird.Device, err error) {
	allRingDevices, _ := bc.getRingData()
	totalWeight := float64(0)

	for _, dev := range allRingDevices {
		totalWeight += dev.Weight
	}

	db, err := bc.getDb()
	if err != nil {
		return nil, err
	}
	//defer db.Close()
	now := time.Now()
	rows, err := db.Query("SELECT Ip, Port, Device, Weight, Mounted, Reachable "+
		"FROM Device WHERE (Mounted=0 OR Reachable=0) AND LastUpdate < ? "+
		"ORDER BY LastUpdate", now.Add(-bc.maxAge))
	defer rows.Close()
	//var qryErrors []error
	//var badDevices []hummingbird.Device

	weightToUnmount := float64(0)
	for rows.Next() {
		var ip, device string
		var port int
		var weight float64
		var mounted, reachable bool

		if err := rows.Scan(&ip, &port, &device, &weight, &mounted, &reachable); err != nil {
			return nil, err
		} else {
			weightToUnmount += weight
			if weightToUnmount < totalWeight*bc.maxWeightChange {
				badDevices = append(badDevices, hummingbird.Device{Ip: ip, Port: port, Device: device, Weight: weight})
			}
		}
	}
	return badDevices, nil
}

func (bc *BirdCatcher) updateRing() (output []string, err error) {
	badDevices, err := bc.getDevicesToUnmount()
	if err != nil {
		return nil, err
	}
	for _, dev := range badDevices {
		devKey := bc.deviceId(dev.Ip, dev.Port, dev.Device)
		cmd := exec.Command(
			"swift-ring-builder", bc.ringBuilder, "set_weight", devKey, "0")
		var out bytes.Buffer
		cmd.Stdout = &out
		if err := cmd.Run(); err != nil {
			return nil, err
		} else {
			output = append(output, out.String())
		}
	}

	cmd := exec.Command("swift-ring-builder", bc.ringBuilder, "rebalance")
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, err
	} else {
		output = append(output, out.String())
	}

	db, err := bc.getDb()
	if err != nil {
		return nil, err
	}
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	//defer db.Close()

	now := time.Now()
	for _, dev := range badDevices {
		_, err = tx.Exec("INSERT INTO RingAction "+
			"(Ip, Port, Device, Action, CreateDate) VALUES "+
			"(?,?,?,?,?)", dev.Ip, dev.Port, dev.Device, "ZEROED", now)
		if err != nil {
			return nil, err

		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return output, nil
}

type ReportDevice struct {
	Ip         string
	Port       int
	Device     string
	Weight     float64
	Mounted    bool
	Reachable  bool
	LastUpdate time.Time
}

type ReportData struct {
	TotalDevices     int
	TotalWeight      float64
	UnmountedDevices []ReportDevice
	LastRingZeroes   []ReportDevice
}

func (bc *BirdCatcher) getReportData() (*ReportData, error) {

	db, err := bc.getDb()
	if err != nil {
		return nil, err
	}
	rows, err := db.Query("SELECT count(*), SUM(Weight) FROM Device WHERE InRing=1")
	defer rows.Close()
	var numDevices int
	var totalWeight float64
	rows.Next()
	if err := rows.Scan(&numDevices, &totalWeight); err != nil {
		return nil, err
	}

	rData := ReportData{
		TotalDevices: numDevices,
		TotalWeight:  totalWeight}

	rows, err = db.Query("SELECT Ip, Port, Device, Weight, Mounted, Reachable, LastUpdate " +
		"FROM Device WHERE (Mounted=0 OR Reachable=0) ORDER BY LastUpdate")

	for rows.Next() {
		var ip, device string
		var port int
		var weight float64
		var lastUpdate time.Time
		var mounted, reachable bool

		if err := rows.Scan(&ip, &port, &device,
			&weight, &mounted, &reachable, &lastUpdate); err != nil {
			return nil, err
		} else {
			rData.UnmountedDevices = append(
				rData.UnmountedDevices,
				ReportDevice{
					Ip: ip, Port: port, Device: device, Weight: weight,
					Mounted: mounted, Reachable: reachable, LastUpdate: lastUpdate})
		}
	}
	rows, err = db.Query("SELECT Ip, Port, Device, Action, CreateDate " +
		"FROM RingAction WHERE " +
		"CreateDate = (SELECT MAX(CreateDate) FROM RingAction) ORDER BY CreateDate")

	for rows.Next() {
		var ip, device, action string
		var port int
		var createDate time.Time

		if err := rows.Scan(&ip, &port, &device, &action, &createDate); err != nil {
			return nil, err
		} else {
			rData.LastRingZeroes = append(
				rData.LastRingZeroes,
				ReportDevice{
					Ip: ip, Port: port, Device: device, LastUpdate: createDate})
		}
	}
	rows.Close()
	return &rData, nil
}

func (bc *BirdCatcher) produceReport() error {
	rData, err := bc.getReportData()
	if err != nil {
		return err
	}
	reportLoc := filepath.Join(bc.reportDir, ReportName)

	d, err := json.Marshal(rData)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(reportLoc, d, 0644)
}

func (bc *BirdCatcher) Run() {
	bc.updateDb()
	if bc.needRingUpdate() {
		bc.updateRing()
	}
	bc.produceReport()
	bc.db.Close()
	bc.db = nil

}

func (bc *BirdCatcher) RunForever() {
	for {
		bc.Run()
		time.Sleep(time.Hour)
	}
}

func GetBirdCatcher(serverconf hummingbird.Config) (*BirdCatcher, error) {

	hashPathPrefix, hashPathSuffix, err := hummingbird.GetHashPrefixAndSuffix()
	if err != nil {
		fmt.Println("Unable to load hash path prefix and suffix:", err)
		return nil, err
	}
	objRing, err := hummingbird.GetRing("object", hashPathPrefix, hashPathSuffix, 0)
	if err != nil {
		fmt.Println("Unable to load ring:", err)
		return nil, err
	}
	workingDir, ok := serverconf.Get("andrewd", "working_dir")
	if !ok {
		panic("Invalid Config, no working_dir")
	}
	reportDir, ok := serverconf.Get("andrewd", "report_dir")
	if !ok {
		panic("Invalid Config, no report_dir")
	}
	maxAge := time.Duration(serverconf.GetInt("andrewd", "max_age_sec", int64(hummingbird.ONE_WEEK*time.Second)))
	maxWeightChange := serverconf.GetFloat("andrewd", "max_weight_change", 0.005)
	ringBuilder := serverconf.GetDefault("andrewd", "ring_builder", "/etc/swift/object.builder")
	daysBetweenRun := serverconf.GetInt("andrewd", "days_between_run", 3)
	durBetweenRun := time.Duration(daysBetweenRun*24) * time.Hour

	bc := &BirdCatcher{
		workingDir:      workingDir,
		reportDir:       reportDir,
		maxAge:          maxAge,
		maxWeightChange: maxWeightChange,
		ringBuilder:     ringBuilder,
		durBetweenRun:   durBetweenRun}
	bc.oring = objRing
	bc.logger = hummingbird.SetupLogger(serverconf.GetDefault(
		"andrewd", "log_facility", "LOG_LOCAL0"), "birdcatcher", "")
	return bc, nil

}
