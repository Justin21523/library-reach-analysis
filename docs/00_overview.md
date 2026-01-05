# 00. 專案總覽（給初學者）

> 這份文件用「初學者也能跟上」的方式，帶你快速理解 LibraryReach 這個專案在做什麼、資料怎麼流、各資料夾負責什麼，以及你應該從哪裡開始讀。
>
> 注意：本專案的「程式碼/註解/README/commit message」都是英文；但本 `docs/` 目錄內是中文教學文件（繁體中文為主）。

---

## 1) 本階段目標（中文）

1. 讓你知道 LibraryReach 的核心問題：如何用公共運輸站點資料，建立「可解釋、可落地」的可近性（accessibility）baseline。
2. 讓你理解專案的架構（ingestion → spatial → scoring → planning → api → web）與資料流。
3. 讓你能跑起來（至少可以跑測試、驗證 catalog、啟動 API/Web），並知道輸出會去哪裡。

---

## 2) 你會看到哪些檔案？為何要這樣拆？（中文）

這個 repo 的設計核心是「模組化 + 可重現（reproducible）」。你可以把它想成一條資料管線：

1. **Catalogs（你自己維護的 CSV）**：圖書館分館、外展候選點（你的 domain data）。
2. **TDX 停靠點資料（外部 API）**：公車站牌、捷運站點等（外部 input）。
3. **Spatial baseline**：用 500m/1km buffer 計算站點密度（stop density）。
4. **Scoring**：把密度指標轉成 0–100 分並產出 Explain（可解釋理由）。
5. **Planning**：用網格找 deserts，並對外展候選點做排序與理由。
6. **API/Web**：把結果用 API 提供給 Web 介面展示與「what-if 參數調整」。

你會最常用到的路徑：

- `config/`：所有可調參數（城市、權重、門檻、情境 scenario）
- `data/catalogs/`：你自己的 CSV（分館與候選點）
- `src/libraryreach/`：主要程式碼
- `data/raw/`：外部資料的落地（例如 TDX stops）
- `data/processed/`：分析輸出（metrics / scores / deserts / recommendations）
- `reports/`：驗證報告（例如 catalog validation）

---

## 3) 核心概念講解（中文，含關鍵術語中英對照）

### 3.1 Baseline 的意思：先求「可解釋、可落地」

本專案 Phase 1 的 baseline 是：

- **Buffer（緩衝區）**：以圖書館為中心畫 500m / 1km 圓形範圍。
- **Stop density（站點密度）**：計算範圍內有多少 bus/metro 停靠點，並換算成 stops per km²（每平方公里站點數）。

為什麼先做這個？

- 你可以直接用「半徑 + 站點數」向非技術的人解釋。
- 計算成本低，不需要複雜路網或時刻表。
- 可快速驗證資料品質（站點資料有沒有漏、座標是否合理）。

### 3.2 Explain（可解釋性）

Explain 不是「漂亮文字」，而是能回答：

- 分數為什麼是這樣？
- 哪一種 mode（bus/metro）貢獻最大？
- 哪個 radius（500/1000）影響最大？
- 用到哪些原始指標（count/density/target/weight）？

### 3.3 Scenario（情境）

Scenario 代表一組「權重/門檻」的設定檔（YAML override），例如：

- 平日（weekday）
- 假日（weekend）
- 放學後（after_school）

同一套資料下，只要換 config，就能產生不同的分數與 deserts。

---

## 4) 程式碼分區塊貼上（Markdown code block）

下面用 `config/default.yaml` 的片段讓你看懂「可調參數」長什麼樣子：

```yaml
buffers:
  radii_m: [500, 1000]

scoring:
  mode_weights:
    bus: 0.6
    metro: 0.4
  radius_weights:
    "500": 0.6
    "1000": 0.4
```

你可以把它理解成：

- 我們看兩個距離尺度：500m 與 1000m
- bus/metro 各占多少重要性
- 近距離（500m）與稍遠距離（1000m）各占多少重要性

---

## 5) 逐段解釋（中文）

### 5.1 參數為什麼要放 `config/`？

因為公共服務規劃很常遇到：

- 不同城市公共運輸密度不同
- 不同時段可用性不同（尖峰/離峰）
- 不同政策有不同的公平性門檻

把參數放 YAML 的好處是：

1. **可重現**：同一份 config + 同一份輸入，就能重跑出同一份輸出。
2. **可討論**：規劃會議時可以直接討論「權重是不是該調整」。
3. **可擴充**：Phase 2 之後（等時刻表、isochrone）也能沿用這種方式。

---

## 6) 常見錯誤與排查（中文）

1. **跑 `fetch-stops` 失敗**
   - 原因：沒有 `.env` 或沒有填 `TDX_CLIENT_ID` / `TDX_CLIENT_SECRET`
   - 解法：複製 `.env.example` → `.env`，填上你的憑證

2. **Web 打開但沒有點**
   - 原因：你還沒跑過 pipeline，所以 `data/processed/` 沒有輸出
   - 解法：先跑 `libraryreach run-all --scenario weekday`（需要 stops.csv）

3. **Catalog 驗證失敗**
   - 原因：CSV 欄位缺少、座標非數字、city 不在 config 等
   - 解法：跑 `libraryreach validate-catalogs`，看 `reports/catalog_validation.md`

---

## 7) 本階段驗收方式（中文 + 英文命令）

先確定你在 repo 根目錄。

1) 跑測試（應該全部通過）：

- `pytest -q`

2) 驗證 catalogs（應該輸出 `OK`，並寫出報告檔）：

- `PYTHONPATH=src python -m libraryreach validate-catalogs --scenario weekday`

3) 啟動 API/Web（能正常啟動，打開 `http://127.0.0.1:8000/` 看到 Control Console）：

- `uvicorn libraryreach.api.main:app --reload`

