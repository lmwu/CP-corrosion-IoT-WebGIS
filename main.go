package main

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/mattn/go-sqlite3"

	mqttserver "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
)

// 🔥 Station 結構升級：包含最新的監測數據，供前端判斷 Pin 點顏色
type Station struct {
	ID         string  `json:"id"`
	Name       string  `json:"name"`
	Lat        float64 `json:"lat"`
	Lng        float64 `json:"lng"`
	Custodian  string  `json:"custodian"`
	LatestPote float64 `json:"latest_pote"` // 最新極化電位
	LatestAC   float64 `json:"latest_ac"`   // 最新 AC 電流密度
}

type SensorData struct {
	Timestamp          string  `json:"timestamp"`
	StationID          string  `json:"station_id"`
	PolarizedPotential float64 `json:"polarized_potential"`
	OnPotential        float64 `json:"on_potential"`
	ACCurrentDensity   float64 `json:"ac_current_density"`
	DCCurrentDensity   float64 `json:"dc_current_density"`
	SpecimenTemp       float64 `json:"specimen_temp"`
	RefElectrode       string  `json:"ref_electrode"`
	CorrosionRate      float64 `json:"corrosion_rate"`
	Note               string  `json:"note"`
}

// 🌧️ 氣象署 API 回傳的 JSON 結構定義 (針對 Past1hr 過去 1 小時雨量)
type CWAResponse struct {
	Records struct {
		Station []struct {
			StationName string `json:"StationName"`
			StationId   string `json:"StationId"`
			ObsTime     struct {
				DateTime string `json:"DateTime"`
			} `json:"ObsTime"`
			GeoInfo struct {
				Coordinates []struct {
					CoordinateName   string `json:"CoordinateName"`
					StationLatitude  string `json:"StationLatitude"`
					StationLongitude string `json:"StationLongitude"`
				} `json:"Coordinates"`
			} `json:"GeoInfo"`
			RainfallElement struct {
				Past1hr struct {
					Precipitation string `json:"Precipitation"`
				} `json:"Past1hr"`
			} `json:"RainfallElement"`
		} `json:"Station"`
	} `json:"records"`
}

// 🌧️ 前端地圖專用的最新雨量資料結構
type RainData struct {
	StationID   string  `json:"station_id"`
	StationName string  `json:"station_name"`
	Lat         float64 `json:"lat"`
	Lng         float64 `json:"lng"`
	ObsTime     string  `json:"obs_time"`
	RainVal     float64 `json:"rain_val"`
}

var db *sql.DB

const (
	dbFilePath    = "./sensor_monitor.gpkg"
	mqttBrokerUrl = "tcp://localhost:1883"
	topicUplink   = "data/station/+"
	topicDownlink = "cmd/station/"

	// 🔑 中央氣象署 API 授權碼
	cwaAuthCode = "CWA-12345678-aabb-4499-3344-9999999999"
)

func main() {
	var err error

	startEmbeddedMQTTBroker()

	db, err = sql.Open("sqlite3", dbFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	initGeoPackage()
	startMqttMiddleware()

	// 🌧️ 啟動雨量背景自動同步排程 (每 30 分鐘檢查一次)
	go func() {
		fetchAndSaveRainfall()
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			fetchAndSaveRainfall()
		}
	}()

	// 🌐 路由清單
	http.HandleFunc("/api/stations", getStations)
	http.HandleFunc("/api/data", getStationData)
	http.HandleFunc("/api/history", getStationHistory)

	// 🌧️ 新增雨量相關 API 路由
	http.HandleFunc("/api/rainfall/latest", getLatestRainfall)
	http.HandleFunc("/api/history/rainfall", getRainfallHistory)

	http.Handle("/", http.FileServer(http.Dir("./static")))

	log.Println("🌐 [金茂防蝕 - 網頁後端] 系統成功啟動，請瀏覽 http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func startEmbeddedMQTTBroker() {
	server := mqttserver.New(nil)
	_ = server.AddHook(new(auth.AllowHook), nil)
	tcp := listeners.NewTCP(listeners.Config{ID: "giga-gold-broker", Address: ":1883"})
	if err := server.AddListener(tcp); err != nil {
		log.Fatalf("❌ 啟動內嵌 MQTT Broker 失敗: %v", err)
	}
	go func() {
		log.Println("🧱 [內嵌 MQTT 郵局] 伺服器已成功就緒，正在監聽連接埠 :1883...")
		if err := server.Serve(); err != nil {
			log.Fatalf("❌ MQTT Broker 異常中斷: %v", err)
		}
	}()
	time.Sleep(500 * time.Millisecond)
}

func initGeoPackage() {
	db.Exec(`CREATE TABLE IF NOT EXISTS gpkg_spatial_ref_sys (srs_name TEXT NOT NULL, srs_id INTEGER NOT NULL PRIMARY KEY, organization TEXT NOT NULL, organization_coordsys_id INTEGER NOT NULL, definition TEXT NOT NULL, description TEXT);`)
	db.Exec(`CREATE TABLE IF NOT EXISTS gpkg_contents (table_name TEXT NOT NULL PRIMARY KEY, data_type TEXT NOT NULL, identifier TEXT UNIQUE, description TEXT, last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')), min_x DOUBLE, min_y DOUBLE, max_x DOUBLE, max_y DOUBLE, srs_id INTEGER);`)
	db.Exec(`CREATE TABLE IF NOT EXISTS gpkg_geometry_columns (table_name TEXT NOT NULL, column_name TEXT NOT NULL, geometry_type_name TEXT NOT NULL, srs_id INTEGER NOT NULL, z TINYINT NOT NULL, m TINYINT NOT NULL, CONSTRAINT pk_geom_cols PRIMARY KEY (table_name, column_name));`)
	db.Exec(`INSERT OR IGNORE INTO gpkg_spatial_ref_sys VALUES ('Undefined cartesian SRS', -1, 'NONE', -1, 'undefined', 'undefined cartesian');`)
	db.Exec(`INSERT OR IGNORE INTO gpkg_spatial_ref_sys VALUES ('Undefined geographic SRS', 0, 'NONE', 0, 'undefined', 'undefined geographic');`)
	db.Exec(`INSERT OR IGNORE INTO gpkg_spatial_ref_sys VALUES ('WGS 84 geographic 2D', 4326, 'EPSG', 4326, 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","6326"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","890"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]', 'WGS 84 geographic');`)
	db.Exec(`CREATE TABLE IF NOT EXISTS stations (fid INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE,name TEXT,lat REAL,lng REAL,custodian TEXT,geom BLOB);`)
	db.Exec(`CREATE TABLE IF NOT EXISTS sensor_data (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp DATETIME, station_id TEXT, polarized_potential REAL, on_potential REAL, ac_current_density REAL, dc_current_density REAL, specimen_temp REAL, ref_electrode TEXT, corrosion_rate REAL, note TEXT);`)

	// 🌧️ 建立雨量歷史資料表 (使用複合主鍵防呆)
	db.Exec(`CREATE TABLE IF NOT EXISTS rainfall_hourly (
		station_id TEXT NOT NULL,
		station_name TEXT NOT NULL,
		lat REAL NOT NULL,
		lng REAL NOT NULL,
		obs_time DATETIME NOT NULL,
		precipitation REAL NOT NULL,
		PRIMARY KEY (station_id, obs_time)
	);`)

	db.Exec(`INSERT OR IGNORE INTO gpkg_contents (table_name, data_type, identifier, description, srs_id) VALUES ('stations', 'features', 'stations', '金茂防蝕監測站空間圖層', 4326);`)
	db.Exec(`INSERT OR IGNORE INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) VALUES ('stations', 'geom', 'POINT', 4326, 0, 0);`)

	// 💡 確保使用您親自設定的 10 個專案測站
	hspStations := []struct {
		id, name, custodian string
		lat, lng            float64
	}{
		{"CP-ZUO-01", "左營港", "新莊乾塢", 22.6822630, 120.2722750},
		{"CP-MAK-01", "馬公港", "146艦隊", 23.5492650, 119.5651250},
		{"CP-KEL-01", "基隆港", "基支部", 25.1434180, 121.7405850},
		{"CP-SUO-01", "蘇澳港", "261戰隊", 24.5969460, 121.8734520},
		{"CP-SD-01", "牛心灣", "海龍", 23.5699850, 119.5149960},
		{"CP-TC-01", "台中港", "CPC", 24.2585780, 120.5005850},
		{"CP-TN-01", "安平港#3", "濱戰", 22.9716710, 120.1741200},
		{"CP-CK-01", "成功漁港", "成功漁會", 23.0972330, 121.3799820},
		{"CP-HP-01", "和平港", "和平工業港", 24.3036290, 121.7668890},
		{"CP-HC-01", "後壁湖漁港", "恆春海巡隊", 21.9450370, 120.7458890},
	}

	for _, st := range hspStations {
		geomBlob := createGpkgPointBlob(st.lng, st.lat)
		db.Exec(`INSERT OR IGNORE INTO stations (id, name, lat, lng, custodian, geom) VALUES (?, ?, ?, ?, ?, ?)`, st.id, st.name, st.lat, st.lng, st.custodian, geomBlob)
	}
}

func createGpkgPointBlob(lng, lat float64) []byte {
	buf := make([]byte, 29)
	buf[0] = 0x47
	buf[1] = 0x50
	buf[2] = 0x00
	buf[3] = 0x01
	binary.LittleEndian.PutUint32(buf[4:8], 4326)
	buf[8] = 0x01
	binary.LittleEndian.PutUint32(buf[9:13], 1)
	binary.LittleEndian.PutUint64(buf[13:21], math.Float64bits(lng))
	binary.LittleEndian.PutUint64(buf[21:29], math.Float64bits(lat))
	return buf
}

func startMqttMiddleware() {
	opts := mqtt.NewClientOptions().AddBroker(mqttBrokerUrl).SetClientID("WebGIS_Middleware")
	opts.SetOnConnectHandler(func(c mqtt.Client) {
		log.Println("✅ [中介軟體] 成功連線上自建的 MQTT Broker")
		c.Subscribe(topicUplink, 1, mqttMessageRecvHandler)
	})
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Printf("⚠️ [中介軟體] 連線失敗: %v", token.Error())
	} else {
		go startModbusPolling(client)
	}
}

func startModbusPolling(client mqtt.Client) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		rows, err := db.Query("SELECT id FROM stations")
		if err != nil {
			continue
		}
		for rows.Next() {
			var stationID string
			if err := rows.Scan(&stationID); err != nil {
				continue
			}
			client.Publish(topicDownlink+stationID, 1, false, "01030000000AC5CD")
		}
		if err := rows.Err(); err != nil {
			log.Printf("⚠️ [中介軟體] 讀取站點資料時發生錯誤: %v", err)
		}
		rows.Close()
	}
}

var mqttMessageRecvHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	var data SensorData
	if err := json.Unmarshal(msg.Payload(), &data); err != nil {
		return
	}
	// 🌟 修正 1：如果設備沒上報時間，後端補償時強制使用 UTC，並轉換為 ISO 8601 (結尾帶 Z)
	if data.Timestamp == "" {
		data.Timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
	} else {
		// (選用防護) 若設備上報的時間沒有 Z，幫它加上，確保寫入 DB 的格式一致
		if !strings.HasSuffix(data.Timestamp, "Z") && !strings.Contains(data.Timestamp, "+") {
			data.Timestamp = strings.ReplaceAll(data.Timestamp, " ", "T") + "Z"
		}
	}

	topicParts := strings.Split(msg.Topic(), "/")
	if len(topicParts) == 3 && data.StationID == "" {
		data.StationID = topicParts[2]
	}

	db.Exec(`INSERT INTO sensor_data (timestamp, station_id, polarized_potential, on_potential, ac_current_density, dc_current_density, specimen_temp, ref_electrode, corrosion_rate, note) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		data.Timestamp, data.StationID, data.PolarizedPotential, data.OnPotential, data.ACCurrentDensity, data.DCCurrentDensity, data.SpecimenTemp, data.RefElectrode, data.CorrosionRate, data.Note)

	log.Printf("💾 [地理包] 成功接收並儲存測站 [%s] 數據", data.StationID)
}

// 🌧️ 核心優化：從中央氣象署 API 同步每小時雨量資料並批次寫入庫
func fetchAndSaveRainfall() {
	if cwaAuthCode == "" {
		return
	}

	log.Println("🌧️ [雨量同步] 開始從中央氣象署同步每小時雨量資料...")
	apiURL := "https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0002-001?Authorization=" + cwaAuthCode

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(apiURL)
	if err != nil {
		log.Println("❌ [雨量同步] 取得氣象署 API 失敗:", err)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	var cwaData CWAResponse
	if err := json.Unmarshal(body, &cwaData); err != nil {
		return
	}

	tx, err := db.Begin()
	if err != nil {
		return
	}

	// 使用 UPSERT 確保不會產生重複資料
	stmt, err := tx.Prepare(`
		INSERT INTO rainfall_hourly (station_id, station_name, lat, lng, obs_time, precipitation)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(station_id, obs_time) 
		DO UPDATE SET precipitation = excluded.precipitation
	`)
	if err != nil {
		tx.Rollback()
		return
	}
	defer stmt.Close()

	count := 0
	for _, st := range cwaData.Records.Station {
		var lat, lng float64
		for _, coord := range st.GeoInfo.Coordinates {
			if coord.CoordinateName == "WGS84" {
				lat, _ = strconv.ParseFloat(coord.StationLatitude, 64)
				lng, _ = strconv.ParseFloat(coord.StationLongitude, 64)
			}
		}

		rainVal, _ := strconv.ParseFloat(st.RainfallElement.Past1hr.Precipitation, 64)
		if rainVal < 0 {
			rainVal = 0
		} // 排除異常代表值

		obsTime := st.ObsTime.DateTime
		if obsTime == "" {
			continue
		}

		// 🌟 修正 2：攔截氣象署帶有 +08:00 的時間，將其轉換為標準 UTC (GMT+0) 再寫入資料庫
		parsedTime, parseErr := time.Parse(time.RFC3339, obsTime)
		if parseErr == nil {
			obsTime = parsedTime.UTC().Format("2006-01-02T15:04:05Z")
		}

		_, err = stmt.Exec(st.StationId, st.StationName, lat, lng, obsTime, rainVal)
		if err == nil {
			count++
		}
	}

	if err := tx.Commit(); err != nil {
		log.Println("❌ [雨量同步] 提交資料庫交易失敗:", err)
	} else {
		log.Printf("✅ [雨量同步] 成功同步並處理 %d 筆每小時雨量測站數據\n", count)
	}
}

// (保留原有的測站與傳感器 API)
func getStations(w http.ResponseWriter, r *http.Request) {
	query := `
		SELECT s.id, s.name, s.lat, s.lng, s.custodian,
			COALESCE(sd.polarized_potential, 0),
			COALESCE(sd.ac_current_density, 0)
		FROM stations s
		LEFT JOIN (
			SELECT station_id, polarized_potential, ac_current_density 
			FROM sensor_data 
			WHERE id IN (SELECT MAX(id) FROM sensor_data GROUP BY station_id)
		) sd ON s.id = sd.station_id
	`
	rows, err := db.Query(query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var list []Station
	for rows.Next() {
		var s Station
		if err := rows.Scan(&s.ID, &s.Name, &s.Lat, &s.Lng, &s.Custodian, &s.LatestPote, &s.LatestAC); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		list = append(list, s)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(list)
}

func getStationData(w http.ResponseWriter, r *http.Request) {
	stationID := r.URL.Query().Get("id")
	var data SensorData
	err := db.QueryRow(`SELECT timestamp, station_id, polarized_potential, on_potential, ac_current_density, dc_current_density, specimen_temp, ref_electrode, corrosion_rate, note FROM sensor_data WHERE station_id = ? ORDER BY timestamp DESC LIMIT 1`, stationID).
		Scan(&data.Timestamp, &data.StationID, &data.PolarizedPotential, &data.OnPotential, &data.ACCurrentDensity, &data.DCCurrentDensity, &data.SpecimenTemp, &data.RefElectrode, &data.CorrosionRate, &data.Note)
	if err != nil {
		http.Error(w, "無監測記錄", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func getStationHistory(w http.ResponseWriter, r *http.Request) {
	stationID := r.URL.Query().Get("id")
	rows, err := db.Query(`
		SELECT timestamp, station_id, polarized_potential, on_potential, ac_current_density, dc_current_density, specimen_temp, ref_electrode, corrosion_rate, note 
		FROM sensor_data 
		WHERE station_id = ? 
		ORDER BY timestamp ASC`, stationID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var list []SensorData = make([]SensorData, 0)
	for rows.Next() {
		var data SensorData
		if err := rows.Scan(&data.Timestamp, &data.StationID, &data.PolarizedPotential, &data.OnPotential, &data.ACCurrentDensity, &data.DCCurrentDensity, &data.SpecimenTemp, &data.RefElectrode, &data.CorrosionRate, &data.Note); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		list = append(list, data)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(list)
}

// 🌧️ 新增 API：取得所有氣象站最新一筆的「每小時雨量」
func getLatestRainfall(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(`
		SELECT station_id, station_name, lat, lng, MAX(obs_time) as obs_time, precipitation 
		FROM rainfall_hourly 
		GROUP BY station_id
	`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var list []RainData = make([]RainData, 0)
	for rows.Next() {
		var d RainData
		if err := rows.Scan(&d.StationID, &d.StationName, &d.Lat, &d.Lng, &d.ObsTime, &d.RainVal); err == nil {
			list = append(list, d)
		}
	}
	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(list)
}

// 🌧️ 新增 API：取得指定單一雨量站的歷史數據
func getRainfallHistory(w http.ResponseWriter, r *http.Request) {
	stationID := r.URL.Query().Get("id")
	rows, err := db.Query(`
		SELECT obs_time, precipitation 
		FROM rainfall_hourly 
		WHERE station_id = ? 
		ORDER BY obs_time ASC`, stationID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type RainHist struct {
		ObsTime       string  `json:"obs_time"`
		Precipitation float64 `json:"precipitation"`
	}
	var list []RainHist = make([]RainHist, 0)
	for rows.Next() {
		var d RainHist
		if err := rows.Scan(&d.ObsTime, &d.Precipitation); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		list = append(list, d)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(list)
}
