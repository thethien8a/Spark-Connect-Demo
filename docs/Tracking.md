# Tracking Tiến Độ

> Cập nhật file này sau mỗi phiên làm việc. Đây là nguồn duy nhất để biết project đang ở đâu.

## Trạng thái hiện tại

| Mục | Giá trị |
| --- | --- |
| Trạng thái tổng thể | M1 hoàn thành, sẵn sàng triển khai M2 |
| Mốc hiện tại | M2 — Kafka và Debezium |
| Đã hoàn thành | M0 compatibility và M1 PostgreSQL nguồn/generator |
| Đang làm | Chuẩn bị Kafka và Debezium |
| Bước tiếp theo | Dựng Kafka Connect/Debezium và đăng ký PostgreSQL connector |
| Cập nhật cuối | 2026-08-30 |

## Tiến độ theo mốc

| Mốc | Nội dung | Trạng thái | Ghi chú |
| --- | --- | --- | --- |
| M0 | Phạm vi và compatibility matrix | `DONE` | Được chốt theo quyết định triển khai; sẽ xác nhận lại smoke tích hợp khi dựng M2/M3 |
| M1 | PostgreSQL nguồn và generator | `DONE` | PostgreSQL 17.6, logical replication, schema, seed và generator đã kiểm thử |
| M2 | Kafka và Debezium | `TODO` | Chưa dựng |
| M3 | MinIO, Catalog, Spark Connect | `TODO` | Chưa dựng |
| M4 | Bronze streaming | `TODO` | Chưa viết job |
| M5 | Silver `orders_current` | `TODO` | Chưa viết `foreachBatch/MERGE` |
| M6 | Gold metrics | `TODO` | Chưa định nghĩa metric bằng code |
| M7 | Recovery và conflict tests | `TODO` | Chưa kiểm thử |
| M8 | COW/MOR benchmark và Airflow maintenance | `TODO` | Chưa đo/chưa tạo DAG |
| M9 | Tính năng nâng cao, observability và demo cuối | `TODO` | Chưa kiểm thử/chưa triển khai |

## Checklist M0

- [ ] Chốt Spark version.
- [ ] Chốt Scala binary version.
- [ ] Chọn Iceberg runtime tương thích.
- [ ] Chọn Kafka connector và PostgreSQL JDBC driver.
- [ ] Pin Docker image/JAR, không dùng `latest`.
- [ ] Tạo `.env.example`, không commit credentials.
- [ ] Chạy smoke test tối thiểu cho Spark + Kafka + Iceberg.

## Checklist hoàn thành project

- [x] PostgreSQL phát sinh INSERT/UPDATE/DELETE hợp lệ.
- [ ] Debezium phát event đúng và Kafka replay được.
- [ ] Bronze lưu được payload gốc và metadata.
- [ ] Silver phản ánh đúng current state.
- [ ] Restart không làm mất hoặc nhân sai dữ liệu.
- [ ] Gold đối chiếu được với nguồn.
- [ ] Conflict tests có kết quả rõ ràng.
- [ ] COW/MOR có benchmark.
- [ ] Airflow maintenance chạy được an toàn.
- [ ] Time travel và schema evolution được kiểm chứng.
- [ ] Có hướng dẫn chạy/recovery và demo end-to-end.

## Nhật ký thay đổi

| Ngày | Thay đổi | Mốc |
| --- | --- | --- |
| 2026-08-29 | Hoàn thiện kế hoạch, chia thành tài liệu con và tạo tracking | M0 |
| 2026-08-30 | Thêm source-postgres, schema CDC, seed và generator; smoke test đạt | M1 |
| 2026-08-30 | Tách hạ tầng độc lập theo folder, giữ Spark trong một module và dùng shared network | M1 |

## Quy ước trạng thái

- `TODO`: chưa bắt đầu.
- `IN_PROGRESS`: đang thực hiện.
- `BLOCKED`: bị chặn, ghi rõ nguyên nhân trong ghi chú.
- `DONE`: đã có code và kiểm thử đạt.
