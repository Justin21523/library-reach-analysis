# 02 Phase 2 — Loading：多城市 Catalog 載入、正規化、驗證

本文件對應「Phase 2：Loading（載入）」模組，目標是讓你就算是初學者，也能理解：

- 我們的 `libraries.csv` / `outreach_candidates.csv` 是如何被讀進來的
- 讀進來之後做了哪些「正規化（normalization）」讓多城市分析更穩定
- 我們如何做「欄位驗證 / 一致性檢查（validation）」並輸出報表

> 注意：本專案的程式碼（含註解）全部是英文；教學文件用中文。  
> 這份文件會直接引用最新程式碼片段，並逐段解釋「做什麼」與「為什麼這樣做」。

---

## 1) 本階段目標（你現在要解決什麼問題）

在 Phase 1 我們需要跑「可解釋 baseline」：用 500m / 1km buffer 來算圖書館周邊的 transit stops 密度，進而算出分數、找出 deserts、推薦外展點。

但在開始算之前，你一定會遇到非常現實的資料問題：

1. 你拿到的表格欄位名稱不一致  
   - 有的人給 `latitude/longitude`，有的人給 `lat/lon`，甚至有人用 `lng`。
2. 文字欄位常常有空白、全形半形、或「台北市 / 臺北市」這類不同寫法  
   - 這會讓你在「按城市分群」或「對照 config」時出現很多莫名其妙的錯。
3. 你需要可驗收的檢查機制（Validation）  
   - 欄位有沒有少？座標是不是數字？ID 有沒有重複？城市代碼是不是在設定檔的 AOI 清單內？
4. 你希望這些問題能被「報表化」  
   - 不是爆一個 stack trace 就結束，而是能輸出 `reports/catalog_validation.md` 讓你知道該修什麼。

所以 Phase 2 的核心就是：**把「載入」與「驗證」變成可重用、可測試、可報表化的模組**。

---

## 2) 這個 Phase 我改了哪些檔案？為什麼要這樣拆？（對應責任分離）

本 Phase 的修改重點是「不改功能、只把載入/驗證模組寫到初學者也能維護」：

- `src/libraryreach/catalogs/load.py`
  - 負責：讀 CSV → 正規化欄位與字串 → 回傳 DataFrame
  - 不負責：判斷哪些欄位必填、座標範圍是否合理（那是 validation 做）
- `src/libraryreach/catalogs/validators.py`
  - 負責：定義驗證規則（schema + quality + config consistency）
  - 輸出：`CatalogValidationResult(errors, warnings, stats)`（不直接寫檔）
- `src/libraryreach/catalogs/validate.py`
  - 負責：把多個 validators 的結果「彙整」成一個 report
  - 另外：可選擇寫出 `reports/catalog_validation.json` / `reports/catalog_validation.md`
- `docs/02_phase2_loading.md`
  - 負責：本文件（教你怎麼理解與驗收本 Phase）

這樣拆的原因是「責任分離（Separation of Concerns）」：

- Loader 只做「資料變乾淨、欄位變一致」，不要在這裡做一堆規則判斷，否則你很難測試也很難重用。
- Validator 只做「規則判斷」，不要混進 IO（寫檔），否則在 API 或測試環境會變得麻煩。
- Orchestrator（validate.py）才是「把結果整合、寫報表」的地方，方便 CLI / API 共用。

---

## 3) 核心概念講解（初學者必懂，含中英對照）

### 3.1 Catalog（資料目錄 / 主資料表）

- Catalog（資料目錄）在這個專案指的是：「由我們自己維護的主資料表」  
  例如 `data/catalogs/libraries.csv`（圖書館分館清單）、`data/catalogs/outreach_candidates.csv`（外展候選點）。
- 它的特性是：**欄位固定、可版本控管、可長期維護**，不同於 TDX 那種外部 API 資料。

### 3.2 Normalization（正規化）

這裡的「正規化」不是數學上的 normalization，而是資料工程常見的「清洗＋統一格式」：

- 統一欄位名稱：`latitude` → `lat`、`lng` → `lon`
- 統一字串：去掉前後空白、避免 `Taipei` vs `Taipei ` 被當成不同值
- 統一城市代碼：把 `臺北市/台北市` 映射成設定檔想要的 `Taipei`
- 統一候選點 type：把 `Community Center` 變成 `community_center`（方便跟 config 對照）

### 3.3 Validation（驗證）與 Errors/Warnings

Validation 不是「修資料」，而是「告訴你資料哪裡不可信」：

- Errors（錯誤）：必須修，否則 pipeline 不應該繼續跑  
  例：缺欄位、ID 重複、lat/lon 不是數字、城市代碼不在 `aoi.cities` 內
- Warnings（警告）：提醒你可能有問題，但不一定要阻擋  
  例：座標超出「台灣常見範圍」的 bounding box（因為專案也支援多城市/跨國，不能直接當錯）

### 3.4 DataFrame（pandas）

- DataFrame 可以想像成「有欄位名稱的 Excel 表格」，但它是程式可操作的結構。
- 我們用 pandas 的理由：
  - 讀 CSV 很成熟
  - 欄位操作（轉型、去空白、value_counts）方便
  - 後續 join / groupby / 統計也會用到

---

## 4) 程式碼導讀：`src/libraryreach/catalogs/load.py`（載入＋正規化）

這支檔案的核心精神是：**把「讀取 CSV」和「統一格式」做得很乾淨，但不做嚴格規則判斷**。

### 4.1 模組開頭：為什麼要把 Load 和 Validate 拆開？

```python
"""
Catalog loading and normalization (Phase 2: Loading).

... (docstring 省略)
"""
```

重點：

- Load 的輸出是一個「乾淨的 DataFrame」，但不是「保證正確的資料」。
- 這樣你可以在不同場景重用 loader：
  - CLI：讀完直接 validate，失敗就輸出報表
  - API：讀完 validate，把結果傳回前端控制台
  - 測試：只測 loader 是否能把欄位/字串正規化

### 4.2 欄位名稱統一：`_rename_common_columns`

```python
def _rename_common_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename: dict[str, str] = {}
    if "lat" not in df.columns and "latitude" in df.columns:
        rename["latitude"] = "lat"
    if "lon" not in df.columns and "longitude" in df.columns:
        rename["longitude"] = "lon"
    if "lon" not in df.columns and "lng" in df.columns:
        rename["lng"] = "lon"
    return df.rename(columns=rename) if rename else df
```

你要理解的點：

- 我們不是直接「強制」所有欄位都改名，而是有條件地做：
  - 只有當 `lat` 不存在但 `latitude` 存在時，才把 `latitude` 改成 `lat`
  - 這樣比較安全：避免你原本就有 `lat`，又同時有 `latitude` 時被覆蓋造成混亂
- `lng` 是前端地圖常見欄位名（例如某些 GIS 工具），我們也支援

### 4.3 字串去空白：`_strip_string_columns`

```python
def _strip_string_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for c in columns:
        if c not in df.columns:
            continue
        df[c] = df[c].astype("string").str.strip()
    return df
```

為什麼一定要 `.str.strip()`？

- 你在 Excel 或 CSV 很容易出現「看不出來」的尾巴空白
- 如果不處理，`"Taipei"` 和 `"Taipei "` 在程式上就是兩個不同值
- 後續 `groupby("city")` 或 `allowed_cities` 檢查會莫名其妙失敗

為什麼用 `astype("string")` 而不是 `astype(str)`？

- pandas 的 `"string"` dtype 可以保留缺值（`<NA>`）
- `astype(str)` 可能把缺值變成字串 `"nan"`，讓你更難做 missing 檢查

### 4.4 城市別名映射：`_normalize_city`

```python
def _normalize_city(df: pd.DataFrame, *, aliases: dict[str, str] | None) -> pd.DataFrame:
    if "city" not in df.columns:
        return df
    if not aliases:
        return df
    alias_map = {str(k).strip(): str(v).strip() for k, v in aliases.items()}

    def map_city(v: Any) -> Any:
        if v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
            return v
        s = str(v).strip()
        return alias_map.get(s, s)

    df["city"] = df["city"].map(map_city)
    return df
```

這段的目的：**多城市分析時，城市代碼要一致**。

- 你的 CSV 可能是中文（`臺北市` / `台北市`）
- 但 TDX API 常用英文字串 city code（例如 `Taipei`）
- 所以我們把「中文輸入」映射到「config 想要的 canonical code」

你會在 `config/default.yaml` 裡看到類似設定：

- `aoi.cities`: 允許分析的城市清單（canonical codes）
- `aoi.city_aliases`: 別名映射表（中文 → canonical）

### 4.5 候選點 type 正規化：`_normalize_candidate_type`

```python
def _normalize_candidate_type(df: pd.DataFrame) -> pd.DataFrame:
    if "type" not in df.columns:
        return df
    t = df["type"].astype("string").str.strip().str.lower()
    t = t.str.replace("-", "_", regex=False).str.replace(" ", "_", regex=False)
    df["type"] = t
    return df
```

原因：

- 你的人類輸入可能是 `Community Center`、`community-center`、`community center`
- 我們把它全部變成 `community_center`
- 這樣才能跟 config 的 `planning.outreach.allowed_candidate_types` 穩定對照

### 4.6 對外 API：讀取兩個 catalog

```python
def load_libraries_catalog(settings: dict[str, Any]) -> pd.DataFrame:
    catalogs_dir = Path(settings["paths"]["catalogs_dir"])
    path = catalogs_dir / "libraries.csv"
    df = pd.read_csv(path)
    df = _rename_common_columns(df)
    df = _strip_string_columns(df, ["id", "name", "address", "city", "district"])
    df = _normalize_city(df, aliases=settings.get("aoi", {}).get("city_aliases"))
    df = _coerce_lat_lon(df)
    if "id" in df.columns:
        df["id"] = df["id"].astype("string").str.strip()
    return df
```

你要抓到的重點：

- 路徑不是寫死的，是從 `settings["paths"]["catalogs_dir"]` 來  
  這樣測試可以用 `tmp_path`，不必真的改你的 repo 資料
- loader 做完正規化就回傳，不做嚴格驗證  
  驗證會在下一個模組做，方便你「先載入、再決定要不要 fail fast」

---

## 5) 程式碼導讀：`src/libraryreach/catalogs/validators.py`（規則檢查）

這支檔案的目標是：**把所有規則整理成「可測試的純函式」**。

### 5.1 結果結構：`CatalogValidationResult`

```python
@dataclass(frozen=True)
class CatalogValidationResult:
    errors: list[str]
    warnings: list[str]
    stats: dict[str, Any]

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0
```

初學者常犯的錯：直接在 validator 裡 `raise`。

我們不這樣做的原因：

- 你會希望一次看到「所有錯誤」，而不是修一個錯誤跑一次
- API/前端想要拿到完整報告（errors + warnings + stats）來顯示

### 5.2 欄位必備檢查：`_require_columns`

```python
def _require_columns(df: pd.DataFrame, required: Iterable[str]) -> list[str]:
    missing = [c for c in required if c not in df.columns]
    return [f"Missing required column: {c}" for c in missing]
```

這段很重要：如果欄位少了，我們要「早點回報」，避免後面操作 `df["city"]` 直接炸掉。

### 5.3 ID 唯一性：`_validate_unique_nonempty_id`

這是資料工程的基礎：**任何要當 key 的欄位都必須唯一且不可空**。

重點行為：

- 空值 → error（因為結果無法回寫對應 row）
- 重複 → error（因為結果會 ambiguous）

### 5.4 座標合理性：`_validate_lat_lon`

你要理解「兩層檢查」：

1) 世界範圍：lat 必須在 -90..90、lon 必須在 -180..180（錯了就是錯）  
2) 台灣常見範圍：18..28 / 116..124（只是 warning）  
   - 因為專案可能分析多城市甚至跨國，不能直接當錯

### 5.5 主驗證器：`validate_libraries_catalog` / `validate_outreach_candidates_catalog`

兩者差異：

- libraries：最小欄位是 `id,name,address,lat,lon,city,district`
- outreach_candidates：多了一個 `type`（政策/規則需要）

而且還會做「跟 config 的一致性」：

- `allowed_cities`：catalog 的 `city` 必須在 `aoi.cities` 內
- `allowed_types`：candidate 的 `type` 必須在 `planning.outreach.allowed_candidate_types` 內

### 5.6 多城市一致性：`validate_multi_city_consistency`

這裡的目標不是「判錯」，而是提醒你：

- 你設定檔說要分析 `["Taipei", "NewTaipei"]`
- 但 catalog 只填了 Taipei（那 NewTaipei 的結果會缺一塊）

所以我們用 warning 形式告訴你「哪個 city 沒資料」。

---

## 6) 程式碼導讀：`src/libraryreach/catalogs/validate.py`（彙整＋寫報表）

### 6.1 為什麼需要 orchestrator？

你會看到我們有三個 validator：

- libraries 自己的規則
- outreach_candidates 自己的規則
- multi-city consistency（跨表一致性）

如果每個 validator 都自己寫檔、自己 raise，你會很難重用。

所以 `validate.py` 做的是：

1. 呼叫各個 validators
2. 合併 errors/warnings
3. 組成一個 report dict（可 JSON 化）
4.（可選）寫出 JSON + Markdown 報表
5.（可選）遇到 errors 就 raise（fail fast）

### 6.2 `validate_catalogs` 的核心流程

```python
def validate_catalogs(
    settings: dict[str, Any],
    *,
    libraries: pd.DataFrame,
    outreach_candidates: pd.DataFrame,
    write_report: bool = True,
    raise_on_error: bool = True,
) -> dict[str, Any]:
    allowed_cities = set(map(str, settings.get("aoi", {}).get("cities", []))) or None
    allowed_types = (
        set(map(str, settings.get("planning", {}).get("outreach", {}).get("allowed_candidate_types", [])))
        or None
    )

    lib_result = validate_libraries_catalog(libraries, allowed_cities=allowed_cities)
    out_result = validate_outreach_candidates_catalog(
        outreach_candidates,
        allowed_cities=allowed_cities,
        allowed_types=allowed_types,
    )
    consistency = validate_multi_city_consistency(
        libraries=libraries,
        outreach_candidates=outreach_candidates,
        configured_cities=list(map(str, settings.get("aoi", {}).get("cities", []))),
    )

    errors = lib_result.errors + out_result.errors + consistency.errors
    warnings = lib_result.warnings + out_result.warnings + consistency.warnings

    report = {
        "ok": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "stats": {
            "libraries": lib_result.stats,
            "outreach_candidates": out_result.stats,
            "consistency": consistency.stats,
        },
    }

    if write_report:
        reports_dir = Path(settings["paths"]["reports_dir"])
        reports_dir.mkdir(parents=True, exist_ok=True)
        (reports_dir / "catalog_validation.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        _write_markdown_report(reports_dir / "catalog_validation.md", report)

    if errors and raise_on_error:
        raise ValueError("Catalog validation failed. See reports/catalog_validation.md for details.")

    return report
```

你要注意的工程細節：

- `ensure_ascii=False`：讓中文可以直接寫在 JSON 裡（不會變成 `\\u53f0\\u5317`）
- `reports_dir.mkdir(..., exist_ok=True)`：避免第一次跑就因為沒有資料夾而寫檔失敗
- `raise_on_error` 是可選：  
  - CLI 會想要 fail（讓 CI 擋掉）
  - API 可能想要不 raise，而是把 report 回傳給前端顯示

---

## 7) 常見錯誤與排查（你最可能踩的坑）

### 7.1 找不到檔案（FileNotFoundError）

現象：

- `libraries.csv` / `outreach_candidates.csv` 不在 `settings["paths"]["catalogs_dir"]` 指到的目錄

排查：

1. 檢查 `config/default.yaml` 的 `project.catalogs_dir`
2. 確認 repo 裡真的有 `data/catalogs/libraries.csv` 等檔案

### 7.2 欄位名不符（Missing required column）

現象：

- 報表出現 `Missing required column: lat` 或 `...: city`

排查：

- 你可以用 loader 支援的常見別名：
  - `latitude/longitude` 會被轉成 `lat/lon`
  - `lng` 會被轉成 `lon`
- 但如果你用的是其他名字（例如 `y/x`），就要自己改 CSV 欄位名

### 7.3 城市代碼不在設定檔（unknown city values）

現象：

- 報表顯示 `unknown city values not in config aoi.cities`

排查：

1. 先看你的 CSV `city` 欄位是不是中文（例如 `臺北市`）
2. 如果是，請在 `config/default.yaml` 加 `aoi.city_aliases`  
   把中文映射到 canonical city code（例如 `Taipei`）
3. 再確認 `aoi.cities` 有包含你要分析的 city code

### 7.4 候選點 type 不在 allowed list（unknown type values）

現象：

- `outreach_candidates: unknown type values ...`

排查：

- loader 會把 `Community Center` 變成 `community_center`
- 你要確保 `config/default.yaml` 的 `planning.outreach.allowed_candidate_types` 包含這個值

### 7.5 座標不是數字或超出範圍

現象：

- `invalid lat/lon (non-numeric or missing)`
- 或 `lat/lon out of valid world bounds`

排查：

- 確認 `lat` 不是文字（例如 `25,033` 這種有逗號就會 parse 失敗）
- 確認沒有把 lon/lat 放反

---

## 8) 本階段驗收方式（中文說明 + 英文命令）

你可以用三個層次驗收：

1) Python 語法檢查（確保沒有語法錯）

```bash
python -m compileall -q src
```

預期結果：沒有任何輸出、exit code = 0

2) 單元測試（確保 loader/validator 行為正確）

```bash
pytest -q
```

預期結果：全部 tests passed

3) 真正跑一次「catalog 驗證」並產出報表

如果你已經安裝 editable 套件（依照 README）：

```bash
libraryreach validate-catalogs --scenario weekday
```

如果你還沒安裝套件、只是想用 repo 直接跑（常見於初學者）：

```bash
PYTHONPATH=src python -m libraryreach validate-catalogs --scenario weekday
```

預期結果：

- Console 會顯示類似 `OK` 或 `FAILED: ...` 的 summary
- 會產生/更新：
  - `reports/catalog_validation.json`
  - `reports/catalog_validation.md`

