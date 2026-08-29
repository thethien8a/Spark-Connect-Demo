# Pipeline CDC Và Lakehouse

## Mốc M4 — Bronze

Luồng:

```text
Kafka → readStream → parse JSON → validate
→ bổ sung Kafka metadata → Bronze Iceberg
```

Việc cần làm:

- Khai báo schema CDC rõ ràng.
- Lưu payload gốc cùng `before`, `after`, `op`.
- Lưu topic, partition, offset, source timestamp và ingest timestamp.
- Ghi message lỗi vào DLQ/bảng lỗi.
- Dùng checkpoint riêng.
- Bắt đầu với micro-batch 5–10 giây.

Bronze là append-only. Không merge mọi event vào Bronze.

**Đạt khi:** truy được một thay đổi PostgreSQL từ key, Kafka offset đến payload Bronze và có thể replay.

## Mốc M5 — Silver current state

Trong `foreachBatch`:

1. Parse và validate event.
2. Tách `after` cho insert/update và `before` cho delete.
3. Deduplicate theo khóa và thứ tự transaction.
4. Tạo staging view.
5. Chạy `MERGE INTO` bảng Silver.
6. Insert/update/delete theo `op`.
7. Ghi input, insert, update, delete và rejected metrics.

Bảng đầu tiên:

```text
silver.orders_current
```

Sau đó mở rộng:

```text
silver.products_current
silver.inventory_current
```

Partition theo thời gian ổn định như ngày tạo đơn, không partition theo status. Chỉ thử Storage Partitioned Joins sau khi có benchmark.

**Test bắt buộc:** insert tạo đúng row, update đổi đúng row, delete loại đúng row, duplicate giữ event mới nhất, restart không làm sai current state.

## Mốc M6 — Gold

Tạo các bảng:

```text
gold.daily_revenue
gold.orders_by_status
gold.inventory_snapshot
```

Trước khi code metric phải chốt:

- Đơn trạng thái nào được tính doanh thu.
- Timezone nghiệp vụ.
- Batch hay streaming aggregate.
- Cách xử lý đơn bị hủy sau khi đã thanh toán.

Đối chiếu Gold với phép tính độc lập trên PostgreSQL nguồn.

## Semantics và recovery

- Kafka giữ event đủ lâu để replay.
- Checkpoint phải persistent.
- Sink Silver phải idempotent.
- Không tuyên bố zero data loss production trên single-broker Docker.
- Sau restart phải đối chiếu số lượng và trạng thái với PostgreSQL nguồn.
