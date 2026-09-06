# Kế Hoạch Triển Khai

## Mục tiêu

Xây dựng pipeline E-commerce gần thời gian thực:

```text
PostgreSQL → Debezium → Kafka → Spark Structured Streaming
→ Iceberg trên MinIO
```

Pipeline phải xử lý được INSERT, UPDATE và DELETE, đồng thời chứng minh được current state, replay, recovery, time travel và bảo trì Iceberg.

Phạm vi project bao gồm toàn bộ roadmap M0-M9: CDC cho `customers`, `products`, `orders` và `order_items`; các tầng Bronze, Silver và Gold; replay, recovery, conflict handling, COW/MOR, Airflow maintenance, time travel, schema/partition evolution và observability. Clickstream và dashboard không thuộc phạm vi đã chốt.

Các mốc chỉ xác định thứ tự triển khai kỹ thuật để hệ thống được xây dựng có kiểm soát; chúng không dùng để cắt giảm hoặc trì hoãn phạm vi project.

## Kiến trúc mục tiêu

```diagram
┌─────────────────┐    ┌──────────┐    ┌──────────┐
│ PostgreSQL      │───▶│ Debezium │───▶│ Kafka    │
│ nguồn giao dịch │    │ CDC      │    │ events   │
└─────────────────┘    └──────────┘    └────┬─────┘
                                           ▼
                                  ┌────────────────┐
                                  │ Spark          │
                                  │ Streaming      │
                                  └──────┬─────────┘
                                         ▼
                                  ┌────────────────┐
                                  │ Iceberg        │
                                  │ Bronze/Silver  │
                                  └──────┬─────────┘
                                         ▼
                                  ┌────────────────┐
                                  │ MinIO          │
                                  │ Parquet files  │
                                  └────────────────┘

          PostgreSQL Catalog ─────── Iceberg metadata
          Kafka Connect workers ──── HAProxy ──── connector REST client
          Spark Connect ─────────── notebook/client
          Airflow ───────────────── maintenance
```

## Tài liệu con

Đọc và triển khai theo thứ tự:

1. [Phạm vi và kiến trúc](Pham_Vi_Va_Kien_Truc.md)
2. [Hạ tầng và nguồn dữ liệu](Ha_Tang_Va_Nguon_Du_Lieu.md)
3. [Pipeline CDC và Lakehouse](Pipeline_CDC_Va_Lakehouse.md)
4. [Tối ưu và vận hành](Toi_Uu_Va_Van_Hanh.md)
5. [Tracking tiến độ](Tracking.md)

## Thứ tự thực hiện

| Mốc | Nội dung | Điều kiện chuyển mốc |
| --- | --- | --- |
| M0 | Chốt phạm vi và compatibility matrix | Biết chính xác version Spark, Scala, Iceberg, Kafka và JDBC |
| M1 | PostgreSQL nguồn và generator | Dữ liệu giao dịch thay đổi hợp lệ |
| M2 | Kafka, Debezium và Connect HA | INSERT/UPDATE/DELETE xuất hiện đúng trong topic; Connect REST có endpoint ổn định |
| M3 | MinIO, Catalog và Spark Connect | Tạo/đọc được Iceberg table từ Spark |
| M4 | Bronze streaming | CDC event được lưu và replay được |
| M5 | Silver current state | `orders_current` đúng sau insert/update/delete |
| M6 | Gold metrics | Doanh thu đối chiếu được với nguồn |
| M7 | Recovery và conflict tests | Restart/replay không làm sai dữ liệu |
| M8 | COW/MOR và maintenance | Có benchmark và Airflow DAG chạy được |
| M9 | Tính năng nâng cao | Time travel, schema/partition evolution đạt |

## Nguyên tắc triển khai

- Không dựng toàn bộ stack rồi mới debug; mỗi mốc phải có smoke test riêng.
- PostgreSQL + generator là nguồn dữ liệu chính; dataset công khai chỉ dùng để seed nếu cần.
- Spark Connect dùng cho notebook và truy vấn tương tác; streaming job dài hạn chạy độc lập.
- Kafka Connect REST API đi qua HAProxy; Kafka data traffic kết nối trực tiếp tới broker.
- Kafka Connect worker phải advertise địa chỉ nội bộ có thể reach được để distributed REST forwarding hoạt động.
- Mỗi bảng Silver chỉ có một streaming writer.
- Chưa tối ưu MOR, SPJ hoặc retention trước khi pipeline đúng và có số liệu đo.
- Mọi thay đổi tiến độ phải cập nhật [Tracking.md](Tracking.md).

## Định nghĩa hoàn thành

- CDC từ PostgreSQL đến Bronze và Silver chạy được.
- Insert/update/delete và restart đã được kiểm thử.
- Iceberg lưu đúng metadata trên MinIO và catalog pointer trên PostgreSQL.
- Có Gold metric, time travel và kiểm tra schema evolution.
- Có benchmark COW/MOR và DAG Airflow maintenance.
- Có hướng dẫn chạy, kiểm tra và recovery đủ để người khác tái hiện.
