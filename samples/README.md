# 样例数据

`day1.csv` / `day2.csv`：2024-05-01 与 2024-05-02 两个连续 UTC 日的闸机记录。
`correction.csv`：对 `E2`（跨午夜出站）的修订号 1 更正，可用来观察两日同时重算。

无 Docker 时本地运行：

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
# 浏览器打开 http://127.0.0.1:8000
```
