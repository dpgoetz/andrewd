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
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/openstack/swift/go/hummingbird"
)

const ONE_DAY = 86400

type BirdCatcher struct {
	oring           hummingbird.Ring
	logger          hummingbird.LowLevelLogger
	workingDir      string
	reportDir       string
	maxAge          time.Duration
	maxWeightChange float64
	ringBuilder     string
	ringUpdateFreq  time.Duration
	runFreq         time.Duration
	db              *sql.DB
	dbl             sync.Mutex
	doNotRebalance  bool
}

type ReconData struct {
	Device  string
	Mounted bool
	ip      string
	port    int
	dev     hummingbird.Device
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
	TotalDevices      int
	TotalWeight       float64
	LastSuccessfulRun time.Time
	UnmountedDevices  []ReportDevice
	LastRingZeroes    []ReportDevice
}

type ipPort struct {
	ip   string
	port int
	up   bool
}

var DbName = "birdcatcher.db"
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
	bc.dbl.Lock()
	defer bc.dbl.Unlock()
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
	defer tx.Rollback()
	sqlCreate := `
		CREATE TABLE IF NOT EXISTS device ( 
			id INTEGER PRIMARY KEY,
			ip VARCHAR(40) NOT NULL,  
			port INTEGER NOT NULL,
			device VARCHAR(40) NOT NULL,  
			in_ring INTEGER NOT NULL,
			weight FLOAT NOT NULL,  
			mounted INTEGER NOT NULL,
			reachable INTEGER NOT NULL,  
			create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  
			last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		); 

		CREATE TABLE IF NOT EXISTS device_log ( 
			deviceId INTEGER,
			mounted INTEGER,
			reachable INTEGER  
			create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			notes VARCHAR(255),  
			FOREIGN KEY (deviceId) REFERENCES device(id)
		); 

		CREATE TABLE IF NOT EXISTS ring_action ( 
			id INTEGER PRIMARY KEY,
			ip VARCHAR(40) NOT NULL,  
			port INTEGER NOT NULL,
			device VARCHAR(40) NOT NULL,  
			action VARCHAR(20) NOT NULL,
			create_date TIMESTAMP NOT NULL
		); 

		CREATE TABLE IF NOT EXISTS run_log ( 
			id INTEGER PRIMARY KEY,
			success INTEGER, notes TEXT,  
			create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		); 

		CREATE TRIGGER IF NOT EXISTS device_last_update 
			AFTER UPDATE ON device FOR EACH ROW  
			BEGIN
				UPDATE device SET last_update = CURRENT_TIMESTAMP  
				WHERE id = OLD.id AND (mounted != OLD.mounted OR reachable != OLD.reachable);
			END; 

		CREATE TRIGGER IF NOT EXISTS device_logger  
			AFTER UPDATE ON device FOR EACH ROW  
			BEGIN
				INSERT INTO device_log (deviceId, mounted, reachable)  
				VALUES (OLD.id, OLD.mounted, OLD.reachable);
			END;`
	_, err = tx.Exec(sqlCreate)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	bc.db = db
	return bc.db, nil
}

func (bc *BirdCatcher) getRingData() (map[string]hummingbird.Device, []ipPort) {
	allRingDevices := make(map[string]hummingbird.Device)
	var allWeightedServers []ipPort
	weightedServers := make(map[string]bool)
	for _, dev := range bc.oring.AllDevices() {
		if dev.Ip == "" {
			continue
		}
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
	rows, err := db.Query("SELECT create_date FROM ring_action ORDER BY create_date DESC LIMIT 1")
	defer rows.Close()
	if err != nil {
		bc.logger.Err(fmt.Sprintf("ERROR: SELECT for needRingUpdate: %v", err))
		return false
	}
	if rows.Next() {
		var createDate time.Time
		if err := rows.Scan(&createDate); err == nil {
			return time.Since(createDate) > bc.ringUpdateFreq
		}
	}
	return false
}

func (bc *BirdCatcher) updateDb() error {
	allRingDevices, allWeightedServers := bc.getRingData()
	unmountedDevices := make(map[string]bool)

	reconDevices, downServers := bc.gatherReconData(allWeightedServers)
	for _, rData := range reconDevices {
		if !rData.Mounted {
			unmountedDevices[bc.deviceId(rData.ip, rData.port, rData.Device)] = rData.Mounted
		}
	}

	db, err := bc.getDb()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, _ := tx.Query("SELECT ip, port, device, weight, mounted FROM device")
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
				_, err = tx.Exec("UPDATE device SET in_ring=0 WHERE ip=? AND port=? AND device=?",
					ip, port, device)
			} else {
				if !notReachable {
					if rDev.Weight != weight || mounted == inUnmounted {
						_, err = tx.Exec("UPDATE device SET weight=?, mounted=? WHERE ip=? AND port=? "+
							"AND device=?", rDev.Weight, !inUnmounted, ip, port, device)
					}
				}
			}
			delete(allRingDevices, dKey)
		}
	}
	for _, ipp := range downServers {
		_, err = tx.Exec("UPDATE device SET reachable=0 "+
			"WHERE ip=? AND port=?", ipp.ip, ipp.port)
	}
	for _, rDev := range allRingDevices {
		dKey := bc.deviceId(rDev.Ip, rDev.Port, rDev.Device)
		_, isUnmounted := unmountedDevices[dKey]
		_, notReachable := downServers[bc.serverId(rDev.Ip, rDev.Port)]
		_, err = tx.Exec("INSERT INTO device (ip, port, device, in_ring, weight, mounted, reachable) "+
			"VALUES (?,?,?,?,?,?,?)",
			rDev.Ip, rDev.Port, rDev.Device, true, rDev.Weight, !isUnmounted, !notReachable)
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	bc.LogInfo("Update DB. %d unmounted. %d unreachable servers. %d new devices",
		len(unmountedDevices), len(downServers), len(allRingDevices))

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
	now := time.Now()
	rows, err := db.Query("SELECT ip, port, device, weight, mounted, reachable FROM device WHERE "+
		"(mounted=0 OR reachable=0) AND last_update < ? ORDER BY last_update", now.Add(-bc.maxAge))
	defer rows.Close()

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
				badDevices = append(badDevices, hummingbird.Device{
					Ip: ip, Port: port, Device: device, Weight: weight})
			}
		}
	}
	return badDevices, nil
}

func (bc *BirdCatcher) updateRing() (outputStr string, err error) {
	badDevices, err := bc.getDevicesToUnmount()
	var output []string
	if err != nil {
		return "", err
	}
	for _, dev := range badDevices {
		devKey := bc.deviceId(dev.Ip, dev.Port, dev.Device)
		cmd := exec.Command(
			"swift-ring-builder", bc.ringBuilder, "set_weight", devKey, "0")
		var out bytes.Buffer
		cmd.Stdout = &out
		if err := cmd.Run(); err != nil {
			return "", err
		} else {
			output = append(output, out.String())
			bc.LogInfo("Setting weight of %s to zero", devKey)
		}
	}

	if bc.doNotRebalance {
		bc.LogInfo("NOT Rebalancing ring")
	} else {
		cmd := exec.Command("swift-ring-builder", bc.ringBuilder, "rebalance")
		var out bytes.Buffer
		cmd.Stdout = &out
		if err := cmd.Run(); err != nil {
			bc.LogInfo("Rebalancing ring failed")
			return "", err
		} else {
			output = append(output, out.String())
			bc.LogInfo("Rebalancing ring")
		}
	}
	db, err := bc.getDb()
	if err != nil {
		return "", err
	}
	tx, err := db.Begin()
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	now := time.Now()
	for _, dev := range badDevices {
		_, err = tx.Exec("INSERT INTO ring_action (ip, port, device, action, create_date) "+
			"VALUES (?,?,?,?,?)", dev.Ip, dev.Port, dev.Device, "ZEROED", now)
		if err != nil {
			return "", err

		}
	}
	if err = tx.Commit(); err != nil {
		return "", err
	}
	bc.LogInfo("Removed %s devices from ring", len(badDevices))
	return strings.Join(output, "\n"), nil
}

func (bc *BirdCatcher) logRun(success bool, errText string) error {
	db, err := bc.getDb()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec("INSERT INTO run_log (success, notes) VALUES (?,?)", success, errText)
	if err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (bc *BirdCatcher) getReportData() (*ReportData, error) {
	db, err := bc.getDb()
	if err != nil {
		return nil, err
	}
	rows, err := db.Query("SELECT count(*), SUM(weight) FROM device WHERE in_ring=1")
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

	rows, err = db.Query("SELECT ip, port, device, weight, mounted, reachable, last_update " +
		"FROM device WHERE (mounted=0 OR reachable=0) ORDER BY last_update")

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
	rows, err = db.Query("SELECT ip, port, device, action, create_date FROM ring_action WHERE " +
		"create_date = (SELECT MAX(create_date) FROM ring_action) ORDER BY create_date")

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

	rows, err = db.Query("SELECT create_date FROM run_log ORDER BY create_date DESC LIMIT 1")
	if err != nil {
		bc.logger.Err(fmt.Sprintf("ERROR: SELECT for run_log report: %v", err))
		return nil, err
	}
	if rows.Next() {
		var createDate time.Time
		if err := rows.Scan(&createDate); err == nil {
			rData.LastSuccessfulRun = createDate
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

func (r *BirdCatcher) LogError(format string, args ...interface{}) {
	r.logger.Err(fmt.Sprintf(format, args...))
}

func (r *BirdCatcher) LogDebug(format string, args ...interface{}) {
	r.logger.Debug(fmt.Sprintf(format, args...))
}

func (r *BirdCatcher) LogInfo(format string, args ...interface{}) {
	r.logger.Info(fmt.Sprintf(format, args...))
}

func (bc *BirdCatcher) Run() {
	bc.LogInfo("AndrewD Starting Run")

	err := bc.updateDb()
	var msg string
	if bc.needRingUpdate() {
		msg, err = bc.updateRing()
	}
	err = bc.produceReport()
	if err != nil {
		msg = fmt.Sprintf("Error: %v", err)
	}
	bc.logRun(err == nil, msg)
	bc.db.Close()
	bc.db = nil
}

func (bc *BirdCatcher) RunForever() {
	for {
		bc.Run()
		time.Sleep(bc.runFreq)
	}
}

func GetBirdCatcher(serverconf hummingbird.Config, flags *flag.FlagSet) (hummingbird.Daemon, error) {
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
	ringUpdateFreq := serverconf.GetInt("andrewd", "ring_update_frequency", 259200)
	runFreq := serverconf.GetInt("andrewd", "run_frequency", 3600)
	doNotRebalance := serverconf.GetBool("andrewd", "do_not_rebalance", false)

	logger, err := hummingbird.SetupLogger(serverconf, flags, "andrewd", "birdcatcher")
	if err != nil {
		panic(fmt.Sprintf("Error setting up logger: %v", err))
	}

	bc := BirdCatcher{
		oring:           objRing,
		workingDir:      workingDir,
		reportDir:       reportDir,
		maxAge:          maxAge,
		maxWeightChange: maxWeightChange,
		ringBuilder:     ringBuilder,
		ringUpdateFreq:  time.Duration(ringUpdateFreq) * time.Second,
		runFreq:         time.Duration(runFreq) * time.Second,
		doNotRebalance:  doNotRebalance,
		logger:          logger,
	}
	return &bc, nil
}
