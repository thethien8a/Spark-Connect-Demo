# Hạ Tầng Và Nguồn Dữ Liệu

## Mốc M0 — Compatibility

Tài liệu cũ đang trộn Spark 4.1.2 với dependency Spark 3.5. Phải chọn một bộ tương thích duy nhất:

```text
Spark ↔ Scala ↔ Iceberg runtime ↔ Kafka connector ↔ PostgreSQL JDBC
```

Việc cần làm:

- Pin Docker image và JAR; không dùng `latest`.
- Smoke test tạo Spark session, đọc Kafka và tạo Iceberg table.
- Đặt credentials trong `.env`/secrets.
- Dùng UTC cho timestamp dữ liệu.

## Mốc M1 — PostgreSQL và generator

1. Tạo service `source-postgres`.
2. Bật WAL/logical replication cho Debezium.
3. Tạo bốn bảng MVP và primary key.
4. Viết seed script.
5. Viết generator tạo order và cập nhật trạng thái theo transition hợp lệ.
6. Tạo các tình huống INSERT, UPDATE, DELETE và dữ liệu lỗi có kiểm soát.

**Đạt khi:** PostgreSQL có dữ liệu thay đổi liên tục, generator chạy lại được và có thể dừng mà không làm hỏng dữ liệu.

## Mốc M2 — Kafka và Debezium

1. Dựng Kafka và Kafka Connect/Debezium.
2. Đăng ký PostgreSQL connector bằng cấu hình ngoài source code.
3. Tạo topic CDC và DLQ.
4. Dùng primary key làm Kafka key.
5. Cấu hình retention đủ cho replay.
6. Kiểm tra `before`, `after`, `op`, source metadata và offset.

**Đạt khi:** một thao tác insert/update/delete trong PostgreSQL xuất hiện đúng ở topic tương ứng và có thể đọc lại.

## Mốc M3 — MinIO và Iceberg Catalog

1. Dựng MinIO và init bucket `iceberg-data` idempotent.
2. Dựng PostgreSQL Catalog riêng hoặc database riêng.
3. Cấu hình warehouse và S3 endpoint nội bộ Docker.
4. Cấu hình region nhất quán, ví dụ `us-east-1`.
5. Đăng ký Iceberg namespaces `bronze`, `silver`, `gold`.
6. Kiểm tra data file/metadata nằm trên MinIO và catalog pointer nằm trên PostgreSQL.

**Đạt khi:** tạo, ghi, đọc và restart thử một Iceberg table mà không mất metadata.

## Cấu hình Spark và Docker

Stack development mục tiêu:

```text
source-postgres
kafka + kafka-connect/debezium
catalog-postgres
minio
spark-master + spark-worker
spark-connect
airflow (giai đoạn maintenance)
```

Nguyên tắc:

- Chung Docker network, gọi nhau bằng service name.
- Persistent volume cho database, Kafka, MinIO và checkpoint.
- Checkpoint tách khỏi warehouse.
- Health check phản ánh khả năng sẵn sàng thật.
- Spark được nạp đúng Iceberg, Kafka và JDBC JAR.

Smoke test Spark Connect:

```python
spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
```

Từ host dùng địa chỉ publish tương ứng; từ container dùng service name nội bộ.
