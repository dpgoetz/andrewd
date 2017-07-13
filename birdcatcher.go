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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/troubling/hummingbird/accountserver"
	"github.com/troubling/hummingbird/client"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/containerserver"
	"github.com/troubling/hummingbird/objectserver"
)

const ONE_DAY = 86400
const SLEEP_TIME = 1 * time.Millisecond //100 * time.Millisecond

type ringData struct {
	r           *ring.Ring
	p           *conf.Policy
	builderPath string
}

type BirdCatcher struct {
	policyToRing       map[int]ringData
	logger             LowLevelLogger
	workingDir         string
	reportDir          string
	maxAge             time.Duration
	maxWeightChange    float64
	ringUpdateFreq     time.Duration
	runFreq            time.Duration
	db                 *sql.DB
	dbl                sync.Mutex
	doNotRebalance     bool
	lastPartProcessed  time.Time
	dispersionCanceler chan struct{}
	partitionProcessed chan struct{}
	onceFullDispersion bool
}

type ReconData struct {
	Device  string
	Mounted bool
	ip      string
	port    int
	dev     ring.Device
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

type probObj struct {
	part       int
	nodesFound int
}

var DbName = "birdcatcher.db"
var ReportName = "unmounted_report_%d.json"

func rescueLonelyPartition(policy int64, partition uint64, goodNode *ring.Device, badNodes []*ring.Device, rescueDone chan struct{}) {
	prjs := []*objectserver.PriorityRepJob{
		{Partition: partition,
			FromDevice: goodNode,
			ToDevices:  badNodes,
			Policy:     int(policy)}}
	objectserver.DoPriRepJobs(prjs, 2, &http.Client{Timeout: time.Hour})
	rescueDone <- struct{}{}
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

func (bc *BirdCatcher) gatherReconData(policy int, servers []ipPort) (devs []*ReconData, downServers map[string]ipPort) {
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

func (bc *BirdCatcher) getDbAndLock() (*sql.DB, error) {
	bc.dbl.Lock()
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
			policy INTEGER NOT NULL,
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
			policy INTEGER NOT NULL,
			ip VARCHAR(40) NOT NULL,  
			port INTEGER NOT NULL,
			device VARCHAR(40) NOT NULL,  
			action VARCHAR(20) NOT NULL,
			create_date TIMESTAMP NOT NULL
		); 

		CREATE TABLE IF NOT EXISTS run_log ( 
			id INTEGER PRIMARY KEY,
			policy INTEGER NOT NULL,
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
			END;

		CREATE TABLE IF NOT EXISTS dispersion_report ( 
			id INTEGER PRIMARY KEY,
			create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			policy INTEGER NOT NULL,
			objects INTEGER NOT NULL,
			objects_found INTEGER NOT NULL,
			report_text TEXT
			);

		CREATE TABLE IF NOT EXISTS dispersion_report_detail ( 
			id INTEGER PRIMARY KEY,
			dispersion_report_id INTEGER NOT NULL,
			policy INTEGER NOT NULL,
			partition INTEGER NOT NULL,
			partition_object_path VARCHAR(100) NOT NULL,
			objects_found INTEGER NOT NULL,
			objects_need INTEGER NOT NULL,
			create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		); 
		`
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

func (bc *BirdCatcher) getRingData(rd ringData) (map[string]ring.Device, []ipPort) {
	oring := *rd.r
	allRingDevices := make(map[string]ring.Device)
	var allWeightedServers []ipPort
	weightedServers := make(map[string]bool)
	for _, dev := range oring.AllDevices() {
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

func (bc *BirdCatcher) needRingUpdate(rd ringData) bool {
	db, err := bc.getDbAndLock()
	defer bc.dbl.Unlock()
	if err != nil {
		bc.logger.Err(fmt.Sprintf("ERROR: getDbAndLock for needRingUpdate: %v", err))
		return false
	}
	rows, err := db.Query("SELECT create_date FROM ring_action WHERE policy = ? ORDER BY create_date DESC LIMIT 1", rd.p.Index)
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
	} else {
		bc.logger.Info(fmt.Sprintf("initial ring update"))
		return true
	}
	return false
}

func (bc *BirdCatcher) updateDb(rd ringData) error {
	policy := rd.p.Index
	allRingDevices, allWeightedServers := bc.getRingData(rd)
	unmountedDevices := make(map[string]bool)

	reconDevices, downServers := bc.gatherReconData(policy, allWeightedServers)
	for _, rData := range reconDevices {
		if !rData.Mounted {
			unmountedDevices[bc.deviceId(rData.ip, rData.port, rData.Device)] = rData.Mounted
		}
	}

	db, err := bc.getDbAndLock()
	defer bc.dbl.Unlock()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, _ := tx.Query(
		"SELECT id, ip, port, device, weight, mounted FROM device WHERE policy = ?", policy)
	defer rows.Close()
	var qryErrors []error
	for rows.Next() {
		var ip, device string
		var dID, port int
		var mounted bool
		var weight float64

		if err := rows.Scan(&dID, &ip, &port, &device, &weight, &mounted); err != nil {
			qryErrors = append(qryErrors, err)
		} else {

			dKey := bc.deviceId(ip, port, device)
			rDev, inRing := allRingDevices[dKey]
			_, inUnmounted := unmountedDevices[dKey]
			_, notReachable := downServers[bc.serverId(ip, port)]
			if !inRing {
				//TODO- handle errors
				_, err = tx.Exec("UPDATE device SET in_ring=0 WHERE id = ?", dID)
			} else {
				if !notReachable {
					if rDev.Weight != weight || mounted == inUnmounted {
						_, err = tx.Exec("UPDATE device SET weight=?, mounted=? "+
							"WHERE id=?", rDev.Weight, !inUnmounted, dID)
					}
				}
			}
			delete(allRingDevices, dKey)
		}
	}
	for _, ipp := range downServers {
		_, err = tx.Exec("UPDATE device SET reachable=0 "+
			"WHERE policy = ? AND ip=? AND port=?", policy, ipp.ip, ipp.port)
	}
	for _, rDev := range allRingDevices {
		dKey := bc.deviceId(rDev.Ip, rDev.Port, rDev.Device)
		_, isUnmounted := unmountedDevices[dKey]
		_, notReachable := downServers[bc.serverId(rDev.Ip, rDev.Port)]
		_, err = tx.Exec("INSERT INTO device (policy, ip, port, device, in_ring, weight, mounted, reachable) "+
			"VALUES (?,?,?,?,?,?,?,?)",
			policy, rDev.Ip, rDev.Port, rDev.Device, true, rDev.Weight, !isUnmounted, !notReachable)
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	bc.LogInfo("Update DB for policy %d. %d unmounted. %d unreachable servers. %d new devices",
		policy, len(unmountedDevices), len(downServers), len(allRingDevices))

	return nil
}

func (bc *BirdCatcher) getDevicesToUnmount(rd ringData) (badDevices []ring.Device, err error) {
	allRingDevices, _ := bc.getRingData(rd)
	policy := rd.p.Index
	totalWeight := float64(0)
	zeroedDevices := make(map[string]bool)
	for _, dev := range allRingDevices {
		totalWeight += dev.Weight
		if dev.Weight == 0 {
			zeroedDevices[bc.deviceId(dev.Ip, dev.Port, dev.Device)] = true
		}
	}
	db, err := bc.getDbAndLock()
	defer bc.dbl.Unlock()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	rows, err := db.Query("SELECT ip, port, device, weight, mounted, reachable "+
		"FROM device WHERE policy = ? AND "+
		"(mounted=0 OR reachable=0) AND last_update < ? ORDER BY last_update",
		policy, now.Add(-bc.maxAge))
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
			if _, alreadyZero := zeroedDevices[bc.deviceId(ip, port, device)]; !alreadyZero {
				weightToUnmount += weight
				if weightToUnmount < totalWeight*bc.maxWeightChange {
					badDevices = append(badDevices, ring.Device{
						Ip: ip, Port: port, Device: device, Weight: weight})
				}
			}
		}
	}
	return badDevices, nil
}

func (bc *BirdCatcher) updateRing(rd ringData) (outputStr string, err error) {
	badDevices, err := bc.getDevicesToUnmount(rd)
	policy := rd.p.Index
	var output []string
	if err != nil {
		return "", err
	}
	for _, dev := range badDevices {
		devKey := bc.deviceId(dev.Ip, dev.Port, dev.Device)
		cmd := exec.Command(
			"swift-ring-builder", rd.builderPath, "set_weight", devKey, "0")
		var out bytes.Buffer
		cmd.Stdout = &out
		if err := cmd.Run(); err != nil {
			return "", err
		} else {
			output = append(output, out.String())
			bc.LogInfo("Setting weight of %s to zero in %s", devKey, rd.builderPath)
		}
		cmd = exec.Command(
			"swift-ring-builder", rd.builderPath,
			"set_info", "--ip", dev.Ip, "--port", strconv.Itoa(dev.Port), "--device", dev.Device,
			"--change-meta", fmt.Sprintf("andrewd zeroed weight on: %s", time.Now().UTC().Format(time.UnixDate)))
		cmd.Stdout = &out
		if err := cmd.Run(); err != nil {
			bc.LogError(fmt.Sprintf("error setting metadata: %s", err))
		}
	}

	if bc.doNotRebalance {
		bc.LogInfo("NOT Rebalancing ring: %s", rd.builderPath)
	} else {
		cmd := exec.Command("swift-ring-builder", rd.builderPath, "rebalance")
		var out bytes.Buffer
		cmd.Stdout = &out
		if err := cmd.Run(); err != nil {
			bc.LogInfo("Rebalancing ring %s failed: %s", rd.builderPath, err)
			return "", err
		} else {
			output = append(output, out.String())
			bc.LogInfo("Rebalancing ring %s", rd.builderPath)
		}
	}
	db, err := bc.getDbAndLock()
	defer bc.dbl.Unlock()
	if err != nil {
		return "", err
	}
	bc.dbl.Lock()
	defer bc.dbl.Unlock()
	tx, err := db.Begin()
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	now := time.Now()
	for _, dev := range badDevices {
		_, err = tx.Exec("INSERT INTO ring_action "+
			"(policy, ip, port, device, action, create_date) "+
			"VALUES (?, ?,?,?,?,?)", policy, dev.Ip, dev.Port, dev.Device,
			"ZEROED", now)
		if err != nil {
			return "", err

		}
	}
	if err = tx.Commit(); err != nil {
		return "", err
	}
	bc.LogInfo("Removed %d devices from ring %s", len(badDevices), rd.builderPath)
	return strings.Join(output, "\n"), nil
}

func (bc *BirdCatcher) logRun(rd ringData, success bool, errText string) error {
	db, err := bc.getDbAndLock()
	defer bc.dbl.Unlock()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec("INSERT INTO run_log (policy, success, notes) VALUES (?, ?,?)", rd.p.Index, success, errText)
	if err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (bc *BirdCatcher) getReportData(rd ringData) (*ReportData, error) {
	db, err := bc.getDbAndLock()
	defer bc.dbl.Unlock()
	if err != nil {
		return nil, err
	}
	policy := rd.p.Index
	rows, err := db.Query("SELECT count(*), SUM(weight) FROM device WHERE policy = ? AND in_ring=1", policy)
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

	rows, err = db.Query("SELECT ip, port, device, weight, mounted, reachable, last_update "+
		"FROM device WHERE policy = ? AND (mounted=0 OR reachable=0) ORDER BY last_update", policy)

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
	rows, err = db.Query("SELECT ip, port, device, action, create_date FROM ring_action WHERE policy = ? AND "+
		"create_date = (SELECT MAX(create_date) FROM ring_action) ORDER BY create_date", policy)

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

	rows, err = db.Query("SELECT create_date FROM run_log WHERE policy = ? ORDER BY create_date DESC LIMIT 1", policy)
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

func (bc *BirdCatcher) produceReport(rd ringData) error {
	rData, err := bc.getReportData(rd)
	if err != nil {
		return err
	}
	reportLoc := filepath.Join(bc.reportDir, fmt.Sprintf(ReportName, rd.p.Index))

	d, err := json.Marshal(rData)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(reportLoc, d, 0644)
}

func (bc *BirdCatcher) logDispersionError(text string) {
	// make a dispersion_report with ^^ as text
	return
}

func (bc *BirdCatcher) PrintLastDispersionReport() {
	db, err := bc.getDbAndLock()
	defer bc.dbl.Unlock()
	if err != nil {
		return
	}

	rows, err := db.Query(`
	SELECT d.policy, d.create_date, d.report_text FROM
	(SELECT policy, MAX(create_date) mcd FROM dispersion_report GROUP BY policy) r
	INNER JOIN dispersion_report d
	ON d.policy = r.policy AND d.create_date = r.mcd`)
	defer rows.Close()
	if err != nil {
		bc.logger.Err(fmt.Sprintf("ERROR: SELECT for dispersion_report: %v", err))
		return
	}
	for rows.Next() {
		var createDate time.Time
		var policy, report string
		if err := rows.Scan(&policy, &createDate, &report); err == nil {
			fmt.Println(fmt.Sprintf(
				"Latest dispersion report for policy %s on %s",
				policy, createDate.Format(time.UnixDate)))
			fmt.Print(report)
			fmt.Println("-----------------------------------------------------")
			// TODO: query to see if there are any objects missing all copies
		} else {
			fmt.Println("Error query data:", err)
		}
	}
}

func (bc *BirdCatcher) makeDispersionReport(policy int64, replicas uint64, goodPartitions int, objsNeed int, objsFound int, probObjects map[string]probObj) {

	objMap := map[int]int{}
	for _, po := range probObjects {
		objMap[po.nodesFound] = objMap[po.nodesFound] + 1
	}
	objText := []struct {
		text string
		cnt  int
	}{}
	for pMissing, cnt := range objMap {
		objText = append(objText, struct {
			text string
			cnt  int
		}{fmt.Sprintf("There were %d partitions missing %d copies.\n", cnt, pMissing), pMissing})
	}
	sort.Slice(objText, func(i, j int) bool { return objText[i].cnt < objText[j].cnt })
	report := fmt.Sprintf("Using storage policy %d\nThere were %d partitions missing 0 copies.\n", policy, goodPartitions)
	for _, t := range objText {
		report += t.text
	}
	report += fmt.Sprintf("%.2f%% of object copies found (%d of %d)\nSample represents 100%% of the object partition space.\n", float64(objsFound*100)/float64(objsNeed), objsFound, objsNeed)

	if bc.onceFullDispersion {
		fmt.Println(report)
		return
	}
	db, err := bc.getDbAndLock()
	defer bc.dbl.Unlock()
	if err != nil {
		return
	}
	tx, err := db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	r, err := tx.Exec("INSERT INTO dispersion_report (policy, objects, objects_found, report_text) VALUES (?,?,?,?)", policy, objsNeed, objsFound, report)
	fmt.Println("333: ", err)
	if err != nil {
		return
	}
	rID, _ := r.LastInsertId()
	recDetail, err := tx.Prepare("INSERT INTO dispersion_report_detail (dispersion_report_id, policy, partition, partition_object_path, objects_found, objects_need) VALUES (?,?,?,?,?)")
	if err != nil {
		return
	}
	for o, po := range probObjects {
		if _, err := recDetail.Exec(rID, policy, po.part, o, po.nodesFound, replicas); err != nil {
			fmt.Println("lalala: ", err)
			return
		}
	}
	fmt.Println("444: ", rID)
	if err = tx.Commit(); err != nil {
		return
	}
	return
}

func (bc *BirdCatcher) scanDispersion(cancelChan chan struct{}) {
	bc.LogInfo("AndrewD Starting Dispersion Check")
	pdc, err := client.NewProxyDirectClient(nil)
	if err != nil {
		bc.logDispersionError(fmt.Sprintf("Could not make client: %v", err))
		return
	}
	c := client.NewProxyClient(pdc, nil, nil)
	resp := c.GetAccount(Account, map[string]string{"format": "json", "prefix": "disp-objs-"}, http.Header{})
	var cRecords []accountserver.ContainerListingRecord
	err = json.NewDecoder(resp.Body).Decode(&cRecords)
	if err != nil {
		bc.logDispersionError(fmt.Sprintf("Could not get container listing: %v", err))
		return
	}
	dirClient := http.Client{Timeout: 10 * time.Second}

	rescueDone := make(chan struct{})
	defer close(rescueDone)
	for _, cr := range cRecords {
		var objRing ring.Ring
		probObjs := map[string]probObj{}
		objsNeed := 0
		objsFound := 0
		goodPartitions := 0
		numRescues := 0
		contArr := strings.Split(cr.Name, "-")
		if len(contArr) != 3 {
			continue
		}
		policy, err := strconv.ParseInt(contArr[2], 10, 64)
		if err != nil {
			bc.logDispersionError(
				fmt.Sprintf("error parsing policy index: %v", err))
			return
		}
		objRing, resp = c.ObjectRingFor(Account, cr.Name)
		if resp != nil {
			bc.logDispersionError(
				fmt.Sprintf("error getting obj ring for: %v", cr.Name))
			return
		}
		marker := ""
		for true {
			select {
			case <-cancelChan:
				return
			default:
			}
			var ors []containerserver.ObjectListingRecord
			resp := c.GetContainer(Account, cr.Name, map[string]string{"format": "json", "marker": marker}, http.Header{})
			err = json.NewDecoder(resp.Body).Decode(&ors)
			if err != nil {
				bc.logDispersionError(fmt.Sprintf("error in container listing: %v", cr.Name))
				return
			}
			if len(ors) == 0 {
				break
			}
			for _, objRec := range ors {
				objArr := strings.Split(objRec.Name, "-")
				if len(objArr) != 2 {
					continue
				}
				partition, e := strconv.ParseUint(objArr[0], 10, 64)
				if e != nil {
					continue
				}
				nodes := objRing.GetNodes(partition)
				goodNodes := []*ring.Device{}
				badNodes := []*ring.Device{}

				for _, device := range nodes {
					url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
						common.Urlencode(Account), common.Urlencode(cr.Name), common.Urlencode(objRec.Name))
					//fmt.Println("making call to: ", url)
					req, err := http.NewRequest("HEAD", url, nil)
					resp, err := dirClient.Do(req)
					//fmt.Println("got back: ", resp.StatusCode)
					if err == nil && resp.StatusCode/100 == 2 {
						goodNodes = append(goodNodes, device)
					} else {
						badNodes = append(badNodes, device)
					}
					if resp != nil {
						resp.Body.Close()
					}
				}
				if len(nodes) != len(goodNodes) {
					probObjs[fmt.Sprintf("%s/%s", cr.Name, objRec.Name)] = probObj{int(partition), len(goodNodes)}
				} else {
					goodPartitions += 1
				}
				if len(nodes) > 1 && len(goodNodes) == 1 {
					go rescueLonelyPartition(policy, partition, goodNodes[0], badNodes, rescueDone)
					numRescues += 1
				}
				if len(goodNodes) == 0 {
					bc.LogError("LOST Partition: %d", partition)
				}
				objsNeed += len(nodes)
				objsFound += len(goodNodes)

				marker = objRec.Name
				if !bc.onceFullDispersion {
					time.Sleep(SLEEP_TIME)
					bc.partitionProcessed <- struct{}{}
				}
			}
		}
		bc.makeDispersionReport(policy, objRing.ReplicaCount(), goodPartitions, objsNeed, objsFound, probObjs)
		bc.LogInfo("Dispersion report written for policy: %d", policy)
		for i := 0; i < numRescues; i++ {
			<-rescueDone
		}

	}
}

func (bc *BirdCatcher) LogError(format string, args ...interface{}) {
	bc.logger.Err(fmt.Sprintf(format, args...))
}

func (bc *BirdCatcher) LogDebug(format string, args ...interface{}) {
	bc.logger.Debug(fmt.Sprintf(format, args...))
}

func (bc *BirdCatcher) LogInfo(format string, args ...interface{}) {
	if bc.onceFullDispersion {
		fmt.Println(fmt.Sprintf(format, args...))
	}
	bc.logger.Info(fmt.Sprintf(format, args...))
}

func (bc *BirdCatcher) dispersionMonitor(ticker <-chan time.Time) {
	for {
		select {
		case <-bc.partitionProcessed:
			bc.lastPartProcessed = time.Now()
		case <-ticker:
			fmt.Println("timer is running- check for dead")
			bc.checkDispersionRunner()
		}
	}
}

func (bc *BirdCatcher) checkDispersionRunner() {
	fmt.Println("doing check")
	if time.Since(bc.lastPartProcessed) > time.Minute { //10*time.Minute {
		if bc.dispersionCanceler != nil {
			fmt.Println("doing close")
			close(bc.dispersionCanceler)
		}
		bc.dispersionCanceler = make(chan struct{})
		fmt.Println("spanning scan")
		go bc.scanDispersion(bc.dispersionCanceler)
	}
}

func (bc *BirdCatcher) runDispersionForever() {
	bc.checkDispersionRunner()
	fmt.Println("run mon")
	bc.dispersionMonitor(time.NewTicker(time.Second * 30).C)
}

func (bc *BirdCatcher) Run() {
	for p, rd := range bc.policyToRing {
		bc.LogInfo("AndrewD Starting Run on policy: %d", p)
		err := bc.updateDb(rd)
		var msg string
		if bc.needRingUpdate(rd) {
			msg, err = bc.updateRing(rd)
		}
		err = bc.produceReport(rd)
		if err != nil {
			msg = fmt.Sprintf("Error: %v", err)
		}
		bc.logRun(rd, err == nil, msg)
	}

	if bc.onceFullDispersion {
		dummyCanceler := make(chan struct{})
		defer close(dummyCanceler)
		bc.scanDispersion(dummyCanceler)
	}

}

func (bc *BirdCatcher) RunForever() {
	bc.onceFullDispersion = false
	go bc.runDispersionForever()
	for {
		bc.Run()
		time.Sleep(bc.runFreq)
	}
}

func GetBirdCatcherDaemon(serverconf conf.Config, flags *flag.FlagSet) (Daemon, error) {
	return GetBirdCatcher(serverconf, flags)
}

func GetBirdCatcher(serverconf conf.Config, flags *flag.FlagSet) (*BirdCatcher, error) {
	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		fmt.Println("Unable to load hash path prefix and suffix:", err)
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
	maxAge := time.Duration(serverconf.GetInt("andrewd", "max_age_sec", int64(common.ONE_WEEK*time.Second)))
	maxWeightChange := serverconf.GetFloat("andrewd", "max_weight_change", 0.01) // TODO make this min 1 drive out of ring up to 1%
	ringLoc := serverconf.GetDefault("andrewd", "ring_location", "/etc/swift/")  // TODO start using hbird ring
	ringUpdateFreq := serverconf.GetInt("andrewd", "ring_update_frequency", 259200)
	runFreq := serverconf.GetInt("andrewd", "run_frequency", 3600)
	doNotRebalance := serverconf.GetBool("andrewd", "do_not_rebalance", false)

	logger, err := SetupLogger(serverconf, flags, "andrewd", "birdcatcher")
	if err != nil {
		panic(fmt.Sprintf("Error setting up logger: %v", err))
	}
	onceFullDispersion := flags.Lookup("full").Value.(flag.Getter).Get().(bool)

	policyList := conf.LoadPolicies()
	pMap := map[int]ringData{}
	for _, p := range policyList {
		objectRing, err := ring.GetRing("object", hashPathPrefix, hashPathSuffix, p.Index)
		if err != nil {
			panic(fmt.Sprintf("Could not load ring with policy", p.Index))
		}
		bPath := fmt.Sprintf("%sobject-%d.builder", ringLoc, p.Index)
		pMap[p.Index] = ringData{&objectRing, p, bPath}
	}

	bc := BirdCatcher{
		policyToRing:       pMap,
		workingDir:         workingDir,
		reportDir:          reportDir,
		maxAge:             maxAge,
		maxWeightChange:    maxWeightChange,
		ringUpdateFreq:     time.Duration(ringUpdateFreq) * time.Second,
		runFreq:            time.Duration(runFreq) * time.Second,
		doNotRebalance:     doNotRebalance,
		logger:             logger,
		partitionProcessed: make(chan struct{}),
		onceFullDispersion: onceFullDispersion,
	}
	return &bc, nil
}
