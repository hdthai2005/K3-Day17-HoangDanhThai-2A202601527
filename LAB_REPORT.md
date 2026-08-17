# Báo cáo thực hiện LAB 17

File này lưu lại toàn bộ các câu lệnh đã chạy, code đã sửa và kết quả tương ứng.

## 0. Setup môi trường
**Lệnh đã chạy:**
```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -q --upgrade pip
.venv\Scripts\pip install -q -r requirements.txt
$env:PYTHONIOENCODING="utf-8"; .venv\Scripts\python seed/generate.py
$env:PYTHONUTF8=1; .venv\Scripts\python tools/run_pipeline.py
```
**Kết quả:** Pipeline hoàn thành, khởi tạo thành công kho DuckDB với dữ liệu Bronze, Silver, Gold.

## 1. Nhiệm vụ 1: Sửa lỗi `gold_training_set`
**Lệnh / File đã sửa:**
- `dbt/models/gold/gold_training_set.sql`: Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`.
- `dags/ai_training_pipeline.py`: Chỉnh `catchup=False` và `max_active_runs=1`.
**Nguyên nhân gốc rễ (Root cause):** Khi không có `unique_key`, lệnh incremental của dbt sẽ mặc định sinh ra câu lệnh `INSERT` (append). Do vậy, khi một ticket được cập nhật nhiều lần sẽ sinh ra các dòng trùng lặp. Thuộc tính `catchup=True` trong DAG làm Airflow chạy bù lịch sử và dồn nhiều active runs gây phình to bảng.
**Kết quả:** Số hàng ổn định ở mức 12,480 hàng, không có duplicate ticket_id sau các lượt chạy.

## 2. Nhiệm vụ 2: Xử lý dữ liệu muộn ở `gold_feature_daily`
**Lệnh đã chạy để đo P99:**
```python
import duckdb
con = duckdb.connect('warehouse.duckdb')
res = con.sql("select quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.99) as p99_ngay from bronze_events;").fetchall()
print("P99:", res[0][0])
```
**P99 thu được:** 2.72 ngày. Dựa trên số này, lookback window chọn là 3 ngày.
**Lệnh / File đã sửa:**
- `dbt/models/gold/gold_feature_daily.sql`: Đổi filter thành `where event_date >= (select max(event_date) - interval 3 day from {{ this }})` và thêm `unique_key = ['event_date', 'customer_id']` để dữ liệu chạy lại cập nhật thay vì nhân bản.
**Nguyên nhân gốc rễ:** Model gốc chỉ cho phép event có ngày LỚN HƠN ngày max hiện tại trong kho. Do vậy, các dữ liệu đến muộn (khoảng 3 ngày theo P99) nhưng thuộc ngày của quá khứ sẽ bị bỏ qua. Việc tăng lookback window giúp quét lại các dòng về muộn này.
**Kết quả:** Số hàng đạt 9,100 hàng (không thừa không thiếu) và ổn định qua nhiều lượt chạy.

## 3. Nhiệm vụ 3: Đổi kiểu cột `priority` và Quarantine
**Lệnh / File đã sửa:**
- `dbt/macros/normalize_priority.sql`: Chuyển `try_cast` thành khối lệnh `CASE WHEN` để map (urgent->1, high->2, medium->3, low->4), còn lại trả về null.
- `dbt/models/silver/silver_tickets.sql`: Thêm câu lệnh filter rác trước CTE `ranked` bằng điều kiện `where priority_clean is not null`.
- `dbt/models/silver/quarantine_tickets.sql`: Lọc các bảng ghi hỏng bằng `where {{ normalize_priority('priority_raw') }} is null`.
- `dbt/models/silver/schema.yml`: Set `enforced: true` trong cấu hình contract và thêm block tests cho valid values (1,2,3,4).
**Nguyên nhân gốc rễ:** Hệ thống nguồn backend đổi schema của giá trị từ integer sang chữ. Nếu dừng toàn bộ pipeline sẽ ảnh hưởng tới 99% data hợp lệ. Do vậy, phải tạo Data Contract và đẩy data không chuẩn (null, 0, 5, P1...) sang quarantine queue.
**Kết quả:** `quarantine_tickets` có chính xác 312 hàng. `silver_tickets` bảo toàn số ticket hợp lệ.

## 4. Bài mở rộng A và B
**Lệnh đã chạy để test Extra:**
```powershell
$env:PYTHONUTF8=1; .venv\Scripts\python seed/generate.py --extra
$env:PYTHONUTF8=1; .venv\Scripts\python tools/compact.py
$env:PYTHONUTF8=1; .venv\Scripts\python tools/explain.py
$env:PYTHONUTF8=1; .venv\Scripts\python tools/crash_test.py
```
**File đã sửa:**
- `tools/compact.py`: Dùng lệnh `COPY ... TO ... PARTITION_BY(event_date)` để chia file lớn thay vì 5000 small files.
- `queries/dashboard.sql`: Sử dụng biến filter theo partition path thay vì strftime.
- `ingest/consumer.py`: Triển khai At-Least-Once = Đảo thứ tự `consumer.commit()` xuống dưới cùng và sử dụng `ON CONFLICT (event_id) DO UPDATE SET ...` để idempotent write.
**Kết quả:** Tốc độ Dashboard tăng vượt trội do Rows Scanned giảm hàng chục lần (nhờ Partition Pruning) và lỗi Duplicate Data khi Consumer Crash được khắc phục hoàn toàn nhờ Idempotency.

## 5. Xác minh (Make verify)
Lệnh đã chạy: `$env:PYTHONUTF8=1; .venv\Scripts\python tools/verify.py`
Tất cả 4/4 tiêu chí đều Pass, pipeline duy trì ổn định checksum qua 3 vòng. 
Đạt 110/100 điểm tuyệt đối.
