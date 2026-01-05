# 01. Phase 1 — Bootstrap（設定載入、目錄初始化、Logging）

> 這一階段的目的，是把專案「跑起來」的地基打穩：設定（config）要怎麼讀、scenario 要怎麼覆寫、runtime 目錄要怎麼建立、log 要怎麼記錄，讓你之後做 ingestion / spatial / scoring / planning 時都能一致、可追蹤、可重現。

---

## 1) 本階段目標（中文）

完成本階段後，你應該能做到：

1. 讀懂 `src/libraryreach/settings.py` 為什麼要這樣寫，以及設定值如何在程式內「一路流下去」。
2. 了解 `.env` 與 YAML 的角色分工（機密 vs 可版本控管的設定）。
3. 了解 logging 的「為什麼」：可重現、可除錯、可驗收。
4. 可以自己修改 `config/default.yaml` 或 scenario 檔，然後跑指令驗證輸出行為。

---

## 2) 我改了哪些檔案？為何要這樣拆？（中文）

本階段主要關心的程式碼模組：

- `src/libraryreach/settings.py`
  - 負責「讀 YAML → 合併 scenario → 載入 `.env` → 建立目錄 → 初始化 logging → 回傳 settings dict」
  - 這是整條資料流（data flow）的源頭：後面所有 pipeline/API 都用同一份 settings

- `src/libraryreach/log.py`
  - 負責設定 logger（console + file）
  - 重要的工程目標：避免重複加 handler（不然 log 會重複印很多次）

對應的設定檔（不算程式碼，但影響流程）：

- `config/default.yaml`
- `config/scenarios/*.yaml`
- `.env`（不進 git）、`.env.example`（模板）

---

## 3) 核心概念講解（中文，含關鍵術語中英對照）

### 3.1 Config 與 Secret 的分離

- **Config（設定）**：可以公開、可討論、可版本控管 → 用 YAML
- **Secret（機密）**：不能進 git → 用 `.env` 或環境變數（environment variables）

在這個專案裡：

- YAML：城市列表、buffer 半徑、權重、門檻、網格大小……
- `.env`：TDX 的 client id / secret

### 3.2 Scenario override（情境覆寫）

Scenario 的概念是：「同一套程式碼 + 同一套輸入資料」下，你只改 config 就能得到不同結果。

這非常適合公共服務規劃：

- 平日/假日、放學後 → 權重可能不同
- deserts threshold（低可達門檻）可能不同

### 3.3 Deep merge（深層合併）

如果你只用「淺層合併」（shallow merge），scenario 一覆寫會整段蓋掉；例如：

- base 有 `scoring.mode_weights`、`scoring.radius_weights`
- scenario 只想改 `scoring.mode_weights`

如果 shallow merge：`scoring` 這個整段會被 scenario 的 `scoring` 整段取代，導致 radius_weights 不見。

所以我們要用 **deep merge**：只覆寫你改的那個 key，其它保留。

### 3.4 Logging（為什麼要有 log？）

對初學者來說 log 看起來像「多餘」，但在資料工程/分析系統裡它是必要的：

1. 你需要知道「這次跑的是哪個 config / 哪個 scenario」。
2. pipeline 一旦出錯，你需要定位是資料、設定、還是程式邏輯的問題。
3. 跟他人協作時，用 log 可以快速交換上下文（context）。

---

## 4) 程式碼分區塊貼上（可用 Markdown code block）

### 4.1 Deep merge（`settings.py`）

```py
def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    # Copy the base mapping so we never mutate caller-owned dictionaries (safer and more predictable).
    merged: dict[str, Any] = dict(base)
    # Iterate override keys so overrides always win for conflicts.
    for key, value in override.items():
        # If both sides are dictionaries, merge recursively so scenarios can override only a nested subset.
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            # Recurse to preserve existing nested keys that are not overridden.
            merged[key] = _deep_merge(merged[key], value)
        else:
            # For non-dicts (or when base is not a dict), the override replaces the base value.
            merged[key] = value
    # Return a new dictionary that represents the merged configuration view.
    return merged
```

### 4.2 `.env` 載入（`settings.py`）

```py
def _load_dotenv_if_present(dotenv_path: Path) -> None:
    # `.env` is optional; if it's missing we simply rely on the existing environment.
    if not dotenv_path.exists():
        return
    # Read the file once and iterate line-by-line so we can support comments and blank lines.
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        # Strip whitespace to avoid accidental spaces becoming part of keys/values.
        line = raw_line.strip()
        # Skip empty lines, comment lines, or malformed lines that are not key/value pairs.
        if not line or line.startswith("#") or "=" not in line:
            continue
        # Split only once so values may legally contain "=" characters.
        key, value = line.split("=", 1)
        # Normalize whitespace around keys to match typical dotenv expectations.
        key = key.strip()
        # Strip whitespace and optional quotes so `.env` can use KEY="value" or KEY='value'.
        value = value.strip().strip('"').strip("'")
        # Do NOT override an already-set environment variable (common pitfall in dev/prod parity).
        os.environ.setdefault(key, value)
```

### 4.3 Logging 設定（`log.py`）

```py
def configure_logging(log_dir: Path, level: str = "INFO") -> logging.Logger:
    # Ensure the log directory exists before we create a file handler.
    log_dir.mkdir(parents=True, exist_ok=True)
    # Use a stable filename so users know where to look for logs across runs.
    log_path = log_dir / "libraryreach.log"

    # Use a named logger so we can control formatting/handlers without touching the root logger.
    logger = logging.getLogger("libraryreach")
    # Normalize log level strings like "info" -> "INFO" to match logging's expectations.
    logger.setLevel(level.upper())
    # Disable propagation so logs are not duplicated by ancestor/root handlers (common pitfall).
    logger.propagate = False

    # Add handlers only once so repeated imports (e.g., uvicorn reload) do not duplicate logs.
    if not logger.handlers:
        # A simple, readable format is ideal for Phase 1 debugging and CLI usage.
        fmt = logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # StreamHandler prints to stderr/stdout so you can see progress in the terminal.
        stream = logging.StreamHandler()
        # Attach the formatter so terminal logs match file logs.
        stream.setFormatter(fmt)
        # Match handler level to logger level so filtering is consistent.
        stream.setLevel(level.upper())

        # FileHandler persists logs so you can audit runs after the fact.
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        # Use the same formatter so logs are comparable across destinations.
        file_handler.setFormatter(fmt)
        # Match handler level to logger level for predictable filtering.
        file_handler.setLevel(level.upper())

        # Attach handlers to the named logger.
        logger.addHandler(stream)
        logger.addHandler(file_handler)

    # Return the configured logger so callers can log immediately after bootstrap.
    return logger
```

---

## 5) 逐段解釋（中文，包含「做什麼 / 為什麼 / 還可以怎麼做」）

### 5.1 `_deep_merge`：做什麼？為什麼？

**這段做什麼**

- 把 base 與 override 合併成一份 dict
- 如果兩邊同一個 key 的 value 都是 dict，就遞迴合併（deep merge）

**為什麼要這樣做**

- scenario override 通常只想改某些欄位（例如 mode_weights）
- deep merge 可以避免「改 A 卻意外把 B 刪掉」

**還可以怎麼做**

- 用第三方套件（例如 deepmerge）更完整，但本專案刻意避免不必要依賴
- 若未來 config 越來越複雜，可以改成 dataclass / pydantic model（會更嚴格、更安全）

### 5.2 `_load_dotenv_if_present`：為什麼不用套件？

**這段做什麼**

- 如果 repo 根目錄有 `.env`，就逐行解析 `KEY=VALUE`
- 只在環境變數尚未存在時才 set（`os.environ.setdefault`）

**為什麼要這樣做**

- `.env` 是 local-only，不進 git，用來放 secret
- `setdefault` 可以避免你在系統環境變數已經設定時，被 `.env` 覆蓋掉（這是一個常見坑）

**常見坑**

- 這個 parser 很簡單，不支援 `export KEY=...` 或多行值（multi-line secrets）
- 但對本專案 Phase 1 來說已足夠（TDX 的 client id/secret）

### 5.3 `configure_logging`：為什麼要避免重複 handler？

**這段做什麼**

- 取得名字叫 `"libraryreach"` 的 logger
- 同時加上：
  - `StreamHandler`（印到 terminal）
  - `FileHandler`（寫到 `logs/libraryreach.log`）
- 用 `if not logger.handlers:` 避免重複加 handler

**為什麼要這樣做**

FastAPI/uvicorn 在 reload 時，module 可能會被重新 import；如果每次 import 都加 handler：

- log 會「重複印 N 次」，你會以為程式跑了 N 次
- debug 會非常痛苦

**還可以怎麼做**

- 用 structured logging（JSON log），方便進 ELK/Datadog（Phase 1 不需要）
- 用 logging config dictConfig（更正式，但對初學者比較難）

---

## 6) 常見錯誤與排查（中文）

1. **Scenario 檔不存在**
   - 現象：程式仍可跑，但只會用 base config
   - 建議：在 `config/scenarios/` 補檔案，或檢查你傳的 `--scenario` 名字

2. **logs/ 無法寫入**
   - 現象：啟動時噴 permission error
   - 解法：確認你在可寫的資料夾中跑；或檢查作業系統權限

3. **你改了 YAML 但看起來沒生效**
   - 現象：結果跟你預期不同
   - 解法：看 terminal log，確認「載入的是哪個 config/scenario 路徑」

---

## 7) 本階段驗收方式（中文 + 英文命令）

1) 編譯檢查（應該沒有 syntax error）：

- `python -m compileall -q src`

2) 跑測試（應該全部通過）：

- `pytest -q`

3) 看 settings 載入 log（應該印出 Loaded settings...）：

- `PYTHONPATH=src python -m libraryreach api-info --scenario weekday`
