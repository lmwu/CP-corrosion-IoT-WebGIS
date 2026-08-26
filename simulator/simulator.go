package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	mqttBrokerUrl  = "tcp://localhost:1883"
	reportInterval = 1 * time.Hour // 測試期間設定每 1 小時動態上報一次
)

// SensorData 上報至 MQTT 的物聯網純數據封包（去除了不需要的 Lat/Lng）
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

// StationConfig 模擬器內部站點配置（移除了 Lat, Lng 欄位）
type StationConfig struct {
	ID        string
	Name      string
	Registers map[uint16]int16 // 現場 Modbus 暫存器映射表
}

var stations []StationConfig

func init() {
	// 🎯 完美瘦身：現場設備只紀錄設備識別碼與暫存器基底，不處理任何空間地圖坐標
	// 地址映射規約：0=極化(*10), 1=通電(*10), 2=AC電流(*10), 3=DC電流(*10), 4=溫度(*10), 5=腐蝕塑率(*1000)
	stations = []StationConfig{
		{"CP-ZUO-01", "左營港", map[uint16]int16{0: 251, 1: -63, 2: 9, 3: -4, 4: 250, 5: 5}},
		{"CP-MAK-01", "馬公港", map[uint16]int16{0: 216, 1: -57, 2: 15, 3: -3, 4: 261, 5: 12}},
		{"CP-KEL-01", "基隆港", map[uint16]int16{0: 235, 1: -52, 2: 13, 3: -2, 4: 242, 5: 3}},
		{"CP-SUO-01", "蘇澳港", map[uint16]int16{0: 243, 1: -57, 2: 8, 3: -3, 4: 275, 5: 22}},
		{"CP-SD-01", "牛心灣", map[uint16]int16{0: 234, 1: -49, 2: 17, 3: -4, 4: 253, 5: 6}},
		{"CP-TC-01", "台中港", map[uint16]int16{0: 222, 1: -61, 2: 9, 3: -5, 4: 258, 5: 8}},
		{"CP-TN-01", "安平港#3", map[uint16]int16{0: 255, 1: -60, 2: 13, 3: -4, 4: 264, 5: 14}},
		{"CP-CK-01", "成功漁港", map[uint16]int16{0: 262, 1: -55, 2: 5, 3: -1, 4: 239, 5: 2}},
		{"CP-HP-01", "和平港", map[uint16]int16{0: 220, 1: -61, 2: 15, 3: -6, 4: 268, 5: 19}},
		{"CP-HC-01", "後壁湖漁港", map[uint16]int16{0: 227, 1: -63, 2: 18, 3: -2, 4: 251, 5: 6}},
	}
}

func main() {
	opts := mqtt.NewClientOptions().AddBroker(mqttBrokerUrl).SetClientID("HSP_Multi_Simulator")
	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("❌ 模擬器無法連線至內嵌 MQTT Broker: %v", token.Error())
	}
	log.Println("🚀 [睦榮興業 feat. 金茂防蝕 - 純數據模擬器] 成功運行，開始傳輸陰極保護感測訊號...")

	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()

	// 啟動當下立刻發送首筆封包
	go publishAllStations(client)

	go func() {
		for range ticker.C {
			publishAllStations(client)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
	log.Println("🛑 模擬器已優雅安全停止。")
}

func publishAllStations(client mqtt.Client) {
	for _, st := range stations {
		// 產生微小的環境噪訊，模擬真實探針的數值跳動
		noisePotential := float64(rand.Intn(10) - 5)
		noiseTemp := float64(rand.Intn(6) - 3)

		// 將暫存器整數根據比例還原為真實物理浮點數並轉為純數據 JSON
		data := SensorData{
			// 🌟 核心修正：加入 .UTC()，強制使用協調世界時 (GMT+0) 進行採樣標記
			Timestamp:          time.Now().UTC().Format("2006-01-02 15:04:05"),
			StationID:          st.ID, // 後端將以此 ID 自動配對 GeoPackage 資料庫中的地圖坐標
			PolarizedPotential: float64(st.Registers[0]) + noisePotential,
			OnPotential:        float64(st.Registers[1]) + noisePotential,
			ACCurrentDensity:   float64(st.Registers[2]) * (0.245 + float64(rand.Intn(10)-5)/100.0),
			DCCurrentDensity:   float64(st.Registers[3]) * 0.032,
			SpecimenTemp:       (float64(st.Registers[4]) + noiseTemp) / 10.0,
			CorrosionRate:      float64(st.Registers[5]) / 1000.0,
			RefElectrode:       "Zn",
			Note:               "工業現場閘道器自動回報",
		}

		jsonPayload, _ := json.Marshal(data)
		topic := "data/station/" + st.ID
		client.Publish(topic, 1, false, jsonPayload)
	}
	// 終端機日誌同樣改為 UTC 顯示，方便與封包內容核對
	log.Printf("📡 [現場端] 已批次推送 10 組純觀測數據至系統 (UTC 時間) - %s", time.Now().UTC().Format("15:04:05"))
}
