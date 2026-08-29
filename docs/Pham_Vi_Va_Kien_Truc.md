# Phạm Vi Và Kiến Trúc

## Use case MVP

Đồng bộ trạng thái đơn hàng từ database giao dịch sang Lakehouse gần thời gian thực và tính doanh thu theo ngày.

Nguồn dữ liệu là PostgreSQL local do project quản lý. Dùng Python/Faker để seed dữ liệu và phát sinh giao dịch liên tục. Không dùng dữ liệu thật.

## Thành phần

| Thành phần | Vai trò |
| --- | --- |
| PostgreSQL nguồn | `customers`, `products`, `orders`, `order_items` |
| Debezium | Đọc WAL/logical replication và tạo CDC event |
| Kafka | Buffer, retention và replay event |
| Spark Structured Streaming | Parse, validate, deduplicate và ghi Iceberg |
| Spark Connect | Client cho notebook, smoke test và truy vấn tương tác |
| Iceberg | Table format, ACID commit, snapshot và schema evolution |
| MinIO | Lưu Parquet, delete files và metadata vật lý |
| PostgreSQL Catalog | Lưu Iceberg metadata pointer |
| Airflow | Điều phối compaction và dọn dẹp định kỳ |

## Mô hình nguồn tối thiểu

```text
customers(customer_id, name, city, created_at)
products(product_id, name, category, price, stock_quantity, created_at, updated_at)
orders(order_id, customer_id, status, total_amount, created_at, updated_at)
order_items(order_id, product_id, quantity, unit_price)
```

`order_id` là khóa chính dùng để đồng bộ `silver.orders_current`.

Trạng thái đơn hàng phải có transition hợp lệ:

```text
pending → paid → shipping → completed
                   └───────→ cancelled
```

## Các tầng Lakehouse

- **Bronze:** CDC payload gốc, `op`, `before`, `after`, source metadata, Kafka topic/partition/offset và ingest timestamp.
- **Silver:** current state của orders, products và inventory.
- **Gold:** `daily_revenue`, `orders_by_status`, `inventory_snapshot`.

Bronze append-only để audit/replay. Silver mới dùng `foreachBatch` và `MERGE INTO`.

## Hợp đồng CDC

| Loại event | Dữ liệu dùng cho Silver |
| --- | --- |
| Insert | `after` |
| Update | `after` |
| Delete | `before` để lấy khóa |
| Snapshot | Dữ liệu khởi tạo |
| JSON/schema lỗi | DLQ hoặc bảng lỗi |

Sink Silver phải idempotent. Dedup theo khóa chính và thứ tự transaction; Kafka offset dùng làm tie-breaker khi cần.

## Vai trò Spark Connect

Spark Connect không phải nguồn dữ liệu và không thay thế Kafka, Iceberg hay MinIO. Nó chỉ cung cấp client API để gửi DataFrame operation/SQL tới Spark Connect Server.

Notebook dùng Connect để kiểm tra dữ liệu và chạy truy vấn. Pipeline dài hạn không được phụ thuộc vào notebook đang mở.
