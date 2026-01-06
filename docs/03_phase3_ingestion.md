# 03 Phase 3 — Ingestion：TDX 站點抓取、Token、快取、離線測試

本文件對應 Phase 3（Ingestion/資料擷取）。你會學到：

1) 我們怎麼用 TDX OAuth（client credentials）拿 access token  
2) 怎麼把 TDX 的站牌/站點資料抓回來，整理成「可供空間分析」的 stops 表  
3) 為什麼要做快取（DiskCache），以及快取帶來的好處與陷阱  
4) 怎麼用「離線單元測試」驗收 ingestion，不需要真的打 TDX 網路

> 專案規則提醒：程式碼與註解全部英文；教學文件中文（繁體）。  
> 本文件會引用對應程式碼區塊，並用初學者可理解的方式逐段解釋「做什麼」與「為什麼」。

---

## 1) 本階段目標（你現在要解決什麼問題）

Phase 1 的 baseline 指標是：  
**每個圖書館分館周邊 500m / 1km 的大眾運輸站點密度（bus/metro）**。

要算這件事，你至少需要一張「站點表（stops table）」：

- 每一列是一個站點（公車站牌 / 捷運站）
- 需要有座標（lat/lon）
- 需要有模式（mode = bus 或 metro），因為分數有權重
- 需要有城市（city），因為我們要做 multi-city 分析

同時，資料來源是 TDX（外部 API），所以你會面臨幾個現實問題：

1) TDX 需要 OAuth token，token 會過期  
2) 站點資料量大，不能每次跑 pipeline 都重新下載（會慢、也會打爆 API）  
3) 對初學者來說，直接在各處寫 `requests.get()` 很難維護與 debug  
4) 我們需要「可離線驗收」：你不一定隨時有網路、也不一定想每次測試都打 TDX

因此 Phase 3 的核心就是：

- 寫一個**可重用**的 `TDXClient`（處理 token + caching + error handling）
- 寫一個 ingestion function `fetch_and_write_stops`（bus + metro → 統一 schema → 寫檔）
- 寫**離線測試**確保邏輯正確（不用真的呼叫 TDX）

---

## 2) 這個 Phase 改了哪些檔案？為什麼要這樣拆？

本 Phase 聚焦「ingestion」模組，修改/新增如下：

- `src/libraryreach/ingestion/tdx_client.py`
  - 負責：TDX OAuth token、GET JSON、OData paging、快取、錯誤訊息統一
  - 目標：讓 ingestion 其它檔案不用知道 token 細節，只要呼叫 `get_json` / `get_paged_json`

- `src/libraryreach/ingestion/fetch_stops.py`
  - 負責：依 config 抓 bus stops（每 city）與 metro stations（每 operator）
  - 把不同 payload 正規化成統一 schema，寫出 `data/raw/tdx/stops.csv`

- `tests/test_ingestion_tdx_client.py`
  - 負責：離線測試 TDXClient 的核心行為（token cache、HTTP cache、401 refresh）

- `tests/test_ingestion_fetch_stops.py`
  - 負責：離線測試 `fetch_and_write_stops` 會寫出 CSV + meta JSON，且 multi-city 迭代正確

為什麼要這樣拆？

- `tdx_client.py` 是**共用基礎建設**：token/caching/error handling 都應該集中管理
- `fetch_stops.py` 是**特定 ingestion 任務**：它只關心「我要拿什麼資料」與「輸出 schema」
- 測試分開是為了：
  - `TDXClient` 測的是 HTTP + token + cache 的規則
  - `fetch_stops` 測的是「資料流」：設定 → 呼叫 client → 正規化 → 寫檔

---

## 3) 核心概念講解（初學者必懂，含中英對照）

### 3.1 Ingestion（資料擷取）

Ingestion 就是把外部資料來源（TDX API）轉成我們專案可用的資料檔。

在這個 repo 的資料流（data flow）中：

1) Ingestion：`data/raw/tdx/stops.csv`（原始但已統一 schema）  
2) Spatial：對 stops 做 buffer join，算密度  
3) Scoring：算 AccessibilityScore + Explain  
4) Planning：找 deserts、推薦外展候選點  
5) API/Web：把結果視覺化與提供控制台

### 3.2 OAuth Client Credentials（OAuth 客戶端憑證流程）

TDX 使用 OAuth2 的 client credentials 流程：

- 你會有 `client_id` + `client_secret`
- 先呼叫 token endpoint 拿到 `access_token`
- 之後呼叫 API 時在 header 加：
  - `Authorization: Bearer <token>`

token 有有效期限（expires_in），所以你必須：

- 快取 token（避免每個 request 都去拿 token）
- 接近過期時提前刷新（避免跑到一半突然 401）

### 3.3 OData Paging（分頁：`$top` / `$skip`）

TDX 的很多 endpoint 支援 OData 參數：

- `$top`: 每頁取多少筆
- `$skip`: 略過多少筆（offset）
- `$format=JSON`: 指定回傳 JSON（避免默認格式差異）

我們用 `get_paged_json()` 來自動分頁，避免一次拉超大 payload 或漏資料。

### 3.4 Disk Cache（磁碟快取）

快取目的：

- 減少重複下載（快）
- 更可重現（reproducible）：同一段時間/同一套參數，資料不會每次都變

但快取也有坑：

- TTL（有效時間）太長會拿到過時資料
- TTL 太短又會一直打 API
- token 快取需要「依 expires_at 判斷」，不能只靠檔案 mtime

---

## 4) 程式碼導讀：`src/libraryreach/ingestion/tdx_client.py`

這支檔案是 ingestion 的核心基礎建設。你可以把它想像成：

> 「我只要給它 path + params，它就會幫我處理 token、重試、快取，回傳 JSON。」

### 4.1 模組目標與定位（docstring）

```python
"""
TDX API client (Phase 3: Ingestion).
...
"""
```

重點：

- **集中管理 token**：其它模組不用自己處理 token 過期、401 refresh
- **集中管理快取**：`DiskCache` 寫到 `cache/`，可刪可重建
- **集中管理錯誤訊息**：出錯時把 status code + body snippet 印出來更好 debug

### 4.2 `TDXClient` 的資料結構（dataclass）

```python
@dataclass
class TDXClient:
    client_id: str
    client_secret: str
    base_url: str
    token_url: str
    cache: DiskCache
    request_timeout_s: int = 30
    logger: logging.Logger | None = None
    session: requests.Session | None = None
```

你要理解的點：

- `cache` 是 ingestion 的關鍵：token 與 GET responses 都會存到磁碟
- `request_timeout_s` 避免 pipeline 因為網路卡住而無限等待
- `session` 可注入（dependency injection）主要是為了測試與連線重用：
  - 測試可以用假 session 回傳假 response（完全離線）
  - 真實跑 ingestion 時可以重用 TCP connection（大量 request 會比較快）

### 4.3 token cache key：為什麼不用把 secret 放進去？

```python
def _token_cache_key(self) -> str:
    return f"{self.client_id}@{self.token_url}"
```

理由：

- secret 不應該被寫進檔名或快取 key（避免意外外洩）
- token cache key 只需要區分不同 client_id 與 token endpoint 就夠了

### 4.4 `get_access_token()`：token 快取 + 60 秒安全緩衝

```python
if token and now_s < (expires_at - 60):
    return str(token)
```

這個「-60 秒」非常重要，是典型的工程細節：

- 你可能在跑 `get_paged_json()`，要連續抓很多頁
- 如果 token 剛好快過期，第一頁拿到後第二頁可能就 401
- 留 60 秒緩衝可以降低「半路過期」的機率（避免 race condition）

### 4.5 `get_json()`：快取、Authorization header、401 retry

```python
headers = {
  "Authorization": f"Bearer {token}",
  "Accept": "application/json",
}
```

工程理由：

- header 統一在 client 內處理，外部模組不用管
- 401 會觸發 token refresh，再 retry 一次（只 retry 一次避免無限循環）

### 4.6 `get_paged_json()`：為什麼要 guardrails？

```python
if page_size <= 0:
    raise ValueError("page_size must be > 0")
if max_pages <= 0:
    raise ValueError("max_pages must be > 0")
```

原因：

初學者常見 bug 是寫出「永遠不會結束的迴圈」：

- endpoint 不支援 `$skip`，每次都回第一頁
- page_size 設錯（0 或負數）
- 參數錯誤導致回傳型別不是 list

有 guardrails 可以讓錯誤更早、更明確。

---

## 5) 程式碼導讀：`src/libraryreach/ingestion/fetch_stops.py`

這支檔案負責「把 bus + metro 合併成 stops 表」。

### 5.1 統一輸出 schema（非常重要）

`fetch_and_write_stops()` 最終會寫出 `stops.csv`，欄位固定為：

- `stop_id`: 站點 ID（字串）
- `name`: 名稱（可空）
- `lat`, `lon`: WGS84 座標
- `city`: 城市代碼（multi-city）
- `mode`: `bus` 或 `metro`
- `source`: 來源（例如 `tdx`、`tdx:TRTC`）

為什麼要固定 schema？

因為後面 spatial/scoring/planning 都應該只依賴這張表，而不是依賴 TDX 的原始 JSON 結構。

### 5.2 `_normalize_bus_stop()` / `_normalize_metro_station()`

你會看到兩個 normalize 函式把不同 payload 統一：

- bus: `StopPosition.PositionLat/PositionLon`
- metro: `StationPosition.PositionLat/PositionLon`（有些版本可能用 StopPosition，所以也兼容）

這就是資料工程常見的「extract → normalize」模式。

### 5.3 multi-city 迭代：`aoi.cities`

```python
cities: list[str] = list(settings.get("aoi", {}).get("cities", []))
```

你要把這件事跟 Phase 2（catalog）連起來：

- catalog 用 `city` 表示城市
- ingestion 用 `aoi.cities` 決定要抓哪些 city 的 bus stops
- 後續 spatial/scoring/planning 都可以按 city 分群輸出

### 5.4 為什麼要寫 `stops.meta.json`？

meta 不是核心資料，但它非常利於驗收與 debug：

- `total`：總筆數
- `counts`：bus/metro 各多少
- `cities`：本次 ingestion 用了哪些城市
- `generated_at_epoch_s`：什麼時間生成（審計/可追溯）

---

## 6) 離線測試：為什麼很重要？怎麼做到？

### 6.1 `tests/test_ingestion_tdx_client.py`

這份測試用假 session / 假 response 來驗證：

- token 有快取時不會打 token endpoint
- token 過期時會打 token endpoint 並更新 cache
- GET 有 cache 時第二次不會打網路
- GET 遇到 401 會 refresh token 並 retry

這類測試的價值是：

- 不依賴網路（CI 或你在飛機上也能跑）
- 不依賴真實 TDX 帳號（避免 secrets 進 CI）
- 能驗證「工程細節」：例如 401 retry、cache hit/miss

### 6.2 `tests/test_ingestion_fetch_stops.py`

這份測試 patch 了 `TDXClient.from_env`，直接回傳 fake client：

- bus：回傳每個城市一筆站牌 + 一筆無效站牌（測試 skip）
- metro：回傳一筆捷運站

最後驗證：

- `stops.csv` 欄位與筆數正確
- `stops.meta.json` 有寫出 counts
- multi-city 下確實「每個 city 都呼叫一次 bus paging」

---

## 7) 常見錯誤與排查（你最可能踩的坑）

### 7.1 沒有設定 TDX credentials（TDXAuthError）

現象：

- 執行 `libraryreach fetch-stops` 時出現：
  - `Missing TDX credentials...`

解法：

1) 複製 `.env.example` → `.env`  
2) 設定 `TDX_CLIENT_ID` 與 `TDX_CLIENT_SECRET`

### 7.2 401 Unauthorized

可能原因：

- token 真的過期（正常，client 會 refresh）
- credentials 無效（token endpoint 可能也會失敗）
- 你的帳號/應用被停用或超額

排查：

- 看 log 是否有 `TDX returned 401, refreshing token`
- 若 refresh 後仍 401，通常是 credentials/權限/配額問題

### 7.3 城市代碼錯誤（TDX endpoint 返回空）

現象：

- bus stops 幾乎空，或最後 `No stops returned from TDX...`

排查：

- 檢查 `config/default.yaml` 的 `aoi.cities` 是否是 TDX 接受的 city codes

### 7.4 快取造成資料看起來「不更新」

現象：

- 你明明改了設定或覺得資料該更新，但結果一直一樣

排查：

- `cache/` 目錄是可刪的：刪掉後重新跑（會重新打 API）
- 或調整 `config/default.yaml` 的 `tdx.cache_ttl_s`

---

## 8) 本階段驗收方式（中文說明 + 英文命令）

### 8.1 離線驗收（推薦：不需要網路）

1) Python 語法檢查：

```bash
python -m compileall -q src
```

2) 跑單元測試（包含 ingestion 離線測試）：

```bash
pytest -q
```

預期結果：tests 全部通過

### 8.2 線上驗收（需要網路 + TDX credentials）

在你已設定 `.env` 且具備網路的情況下：

```bash
libraryreach fetch-stops --scenario weekday
```

預期結果：

- 產生 `data/raw/tdx/stops.csv`
- 產生 `data/raw/tdx/stops.meta.json`

接著你可以跑 Phase 1 pipeline（若你不想重新抓，使用 `--skip-fetch`）：

```bash
libraryreach run-all --scenario weekday --skip-fetch
```

