# 04 Phase 4 — Spatial：投影、Buffer、KD-Tree 半徑查詢（可解釋 baseline）

本文件對應 Phase 4（Spatial/空間計算）。你會學到：

1) 為什麼 baseline 可以不用 GIS 大套件，也能做出可落地的空間分析  
2) 什麼是「局部投影（equirectangular）」以及我們怎麼用 reference latitude 控制誤差  
3) 什麼是 KD-Tree（k-dimensional tree），為什麼適合做「半徑內鄰居查詢」  
4) 怎麼把「半徑內站點數」轉成「密度（stops per km²）」並用在 scoring  
5) 為什麼 UI 的 buffer polygon 只用來「展示/解釋」，而不是用來做精確 overlay

> 專案規則提醒：程式碼與註解全部英文；教學文件中文（繁體）。  
> 本文件會引用對應程式碼區塊，並用初學者可理解的方式逐段解釋「做什麼」與「為什麼」。

---

## 1) 本階段目標（你現在要解決什麼問題）

你在 Phase 1 的 baseline 指標其實非常直覺：

> 「以圖書館為中心 500m / 1km 內，公車站牌 + 捷運站點有多少？密度如何？」

要把這件事做成可落地系統，你需要三個「最低限度但很可靠」的空間能力：

1) 把 WGS84（lat/lon）轉成「可算距離」的平面座標（x/y meters）  
2) 能快速回答：「某點半徑 r 公尺內有哪些站點？」  
3) 能用一致的方式產出 buffer polygon 給地圖顯示（讓使用者看得懂）

Phase 4 就是把這三件事做成：

- 依賴少（沒有 shapely/geopandas/pyproj）
- 可解釋（數學與工程都簡單）
- 可重現（deterministic）
- 足夠準（對 500m/1km 的短距離分析誤差可接受）

---

## 2) 這個 Phase 改了哪些檔案？為什麼要這樣拆？

本 Phase 聚焦 `src/libraryreach/spatial/`：

- `src/libraryreach/spatial/crs.py`
  - 負責：lat/lon ↔ x/y meters 的轉換（局部投影）
  - 目標：提供「距離計算的共同語言」，讓 joins/buffers 用同一套投影假設

- `src/libraryreach/spatial/joins.py`
  - 負責：核心 join：point（libraries）× stops 的半徑計數與密度
  - 技術：cKDTree 半徑查詢（非常快、很適合 baseline）

- `src/libraryreach/spatial/buffers.py`
  - 負責：把「圓形 buffer」近似成 polygon（GeoJSON），給 Web 地圖顯示
  - 重點：這是「視覺化/可解釋」用途，不是精確 GIS overlay

為什麼要拆成三個檔案？

- CRS（投影/座標轉換）是「基礎數學工具」：不應該混在 join 或 UI buffer 裡
- Joins（計數與密度）是「分析核心」：要獨立可測、可重用（CLI、API 都會用）
- Buffers（polygon）是「輸出/展示」：方便給 web map 用，也方便報表截圖解釋

---

## 3) 核心概念講解（初學者必懂，含中英對照）

### 3.1 WGS84 / lat-lon（經緯度）

你在 `libraries.csv`、TDX `stops.csv` 看到的 `lat/lon` 是：

- lat：緯度（latitude）
- lon：經度（longitude）

它們是角度，不是距離（不是公尺）。

所以你不能直接做：

- `sqrt((lat1-lat2)^2 + (lon1-lon2)^2)`

因為「同樣 0.01 度」在不同緯度代表不同的實際距離。

### 3.2 Equirectangular projection（等距矩形投影，局部近似）

對於「短距離（<= 1km）」的 baseline，我們用一個非常簡單的近似：

- 把地球當球體（Earth radius R）
- 在某個 reference latitude 附近，把 lat/lon 轉成平面 x/y（公尺）

概念式：

- `x = R * lon_rad * cos(ref_lat_rad)`
- `y = R * lat_rad`

這個近似在小範圍內非常好用：

- 計算快
- 誤差可控
- 易於解釋（報表也能講得清楚）

### 3.3 Reference latitude（參考緯度）

你會看到我們要選一個 `reference_lat_deg`，原因是：

- 經度縮放需要乘 `cos(lat)`
- 如果每個點用不同 lat，會變複雜（也不好解釋）

所以我們選一個代表性的緯度（mean 或 median），讓整個 AOI 都用同一個縮放。

### 3.4 KD-Tree（k-dimensional tree）

KD-Tree 是一種資料結構，用來加速「鄰近查詢」。

在本專案裡我們做的是：

> 給定每個 library 的 x/y，找出 radius r 內的 stops（也是 x/y）

如果你用最直覺的雙層迴圈：

- 1000 個 libraries × 50000 個 stops = 5 千萬次距離計算（很慢）

KD-Tree 可以把查詢變得非常快，特別適合「很多 stops、很多查詢點、固定半徑」這種問題。

### 3.5 Density（密度：stops per km²）

為什麼不只用 stop count？

因為不同半徑（500m vs 1000m）面積不同：

- 500m 面積 = π*(0.5km)²
- 1000m 面積 = π*(1km)²

如果不轉成密度，兩個半徑的數值很難比較，也很難做權重加總。

---

## 4) 程式碼導讀：`src/libraryreach/spatial/crs.py`（投影：lat/lon ↔ meters）

### 4.1 模組目的

```python
"""
Lightweight coordinate helpers for short-range spatial analysis (Phase 4: Spatial).
...
"""
```

你要抓到的關鍵：

- 我們故意不引入 pyproj/shapely：Phase 1 只要短距離 buffer
- 我們只要能穩定做到「公尺尺度的距離」

### 4.2 `latlon_to_xy_m()` 的核心概念

```python
x = EARTH_RADIUS_M * lon_rad * math.cos(ref_lat_rad)
y = EARTH_RADIUS_M * lat_rad
```

重點：

- lon 需要乘 cos(ref_lat)（經線在高緯度更密集）
- 這是局部近似，對 500m/1km 的距離非常合理

### 4.3 `choose_reference_lat_deg()`

reference latitude 的選擇策略：

- mean：容易解釋、一般情況很穩
- median：對 outliers 比較不敏感

config 的 `spatial.distance.reference_lat_strategy` 目前用 mean（baseline）。

---

## 5) 程式碼導讀：`src/libraryreach/spatial/joins.py`（KD-Tree 半徑計數 + 密度）

### 5.1 整體流程

你可以把 `compute_point_stop_density()` 想像成：

1) 檢查欄位  
2) dropna（沒有座標就不能算）  
3) 選 reference latitude  
4) lat/lon → x/y meters  
5) stops 建 KD-tree  
6) 對每個半徑 r，查鄰居、算 bus/metro counts  
7) counts → density（per km²）  
8) 回傳一張 metrics 表

### 5.2 為什麼用 cKDTree 而不是用 polygon overlay？

因為我們的 baseline 是圓形 buffer，核心問題是「距離」。

- KD-tree 的 radius query 直接用距離定義
- polygon overlay 需要幾何運算套件，依賴多、也比較難解釋

### 5.3 為什麼要輸出 `reference_lat_deg`？

這是可追溯性（traceability）：

- 你在報表或 debug 時可以知道這次投影用哪個 reference latitude
- UI 要畫 buffer polygon 也可以用同一個 reference latitude，避免展示與計算不一致

---

## 6) 程式碼導讀：`src/libraryreach/spatial/buffers.py`（給 UI 的 buffer polygon）

### 6.1 重要觀念：polygon 主要用於展示

`circle_polygon_lonlat()` 產生的 polygon 是「近似圓」，主要用途：

- Web 地圖顯示「我在算哪個範圍」
- 解釋報表（截圖/互動）

真正的計數不是用 polygon overlay，而是 joins 的 KD-tree radius query。

### 6.2 為什麼要用同一個 reference latitude？

因為：

- joins 用 reference latitude 做距離
- buffers 也用 reference latitude 做圈圈

這樣 UI 的圈圈和實際計數邏輯才一致（避免「看起來在圈內，但實際算不在」的混亂）。

---

## 7) 常見錯誤與排查（你最可能踩的坑）

### 7.1 `radii_m` 包含 0 或負數

症狀：

- 可能出現 division by zero 或密度無限大

現在我們在 `joins.py` 加了 guard：r 必須 > 0。

### 7.2 lat/lon 欄位有缺值

症狀：

- 某些 library 或 stop 沒有座標，會被 dropna 丟掉

排查：

- 先用 catalog validation 檢查（Phase 2）
- 檢查 `stops.csv` 是否有 lat/lon 缺值

### 7.3 對大範圍（例如 50km）誤差變大

提醒：

- equirectangular 是局部近似，距離越大誤差越明顯
- Phase 1 的 baseline 只做 500m/1km，完全 OK
- 如果你未來要做 30min isochrone，那會是另一個 Phase（需要更嚴謹的 routing/GTFS）

---

## 8) 本階段驗收方式（中文說明 + 英文命令）

1) Python 語法檢查：

```bash
python -m compileall -q src
```

2) 單元測試：

```bash
pytest -q
```

預期結果：全部 tests passed

