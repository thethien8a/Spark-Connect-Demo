# Tracking Tiến Độ

> Cập nhật file này sau mỗi phiên làm việc. Đây là nguồn duy nhất để biết project đang ở đâu.

## Trạng thái hiện tại

| Mục | Giá trị |
| --- | --- |
| Trạng thái tổng thể | M1 hoàn thành, M2 đang triển khai theo từng bước |
| Mốc hiện tại | M2 — Kafka và Debezium |
| Đã hoàn thành | M0 compatibility và M1 PostgreSQL nguồn/generator |
| Đang làm | Kafka web UI chỉ đọc đã healthy tại `http://localhost:8084`, nhận ba broker, năm topic CDC/DLQ và Connect qua HAProxy; topic manifest vẫn khớp |
| Bước tiếp theo | Chuẩn bị dữ liệu nguồn để kiểm thử snapshot và đăng ký PostgreSQL connector cho bốn bảng qua HAProxy |
| Cập nhật cuối | 2026-11-08 |

## Tiến độ theo mốc

| Mốc | Nội dung | Trạng thái | Ghi chú |
| --- | --- | --- | --- |
| M0 | Phạm vi và compatibility matrix | `DONE` | Được chốt theo quyết định triển khai; sẽ xác nhận lại smoke tích hợp khi dựng M2/M3 |
| M1 | PostgreSQL nguồn và generator | `DONE` | PostgreSQL 17.6, logical replication, schema, seed và generator đã kiểm thử |
| M2 | Kafka và Debezium | `IN_PROGRESS` | 3 Kafka + 2 Connect + HAProxy đã kiểm thử; năm topic có manifest/script; Kafbat UI chỉ đọc đã chạy và kiểm tra HTTP/API; chưa đăng ký CDC connector |
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
| 2026-08-31 | Chốt topology production-like 3 Kafka + 2 Connect và pin Kafka 4.3.0/Debezium 3.6.1.Final | M2 |
| 2026-09-01 | Thêm HAProxy làm endpoint health-aware cho hai Kafka Connect REST API | M2 |
| 2026-09-02 | Chốt toàn bộ roadmap M0-M9 là phạm vi project; loại clickstream, dashboard và cách triển khai rút gọn | M0-M9 |
| 2026-09-02 | Dựng cụm ba Kafka 4.3.0 KRaft node với quorum, health check và volume độc lập | M2 |
| 2026-09-12 | Build custom Kafka Connect image, xác minh SHA-512 và nhận diện Debezium PostgreSQL `3.6.1.Final` qua REST API | M2 |
| 2026-11-06 | Thêm `connect-1`/`connect-2` vào Compose; smoke test đạt REST/plugin, cùng Connect group, REST giữa worker và internal topics RF=3/compaction; chưa đăng ký CDC connector | M2 |
| 2026-11-08 | Thêm HAProxy `connect-lb` tại `http://localhost:8083`; kiểm thử routing tới cả hai worker, dừng từng worker vẫn trả HTTP 200, dừng cả hai trả HTTP 503 và khôi phục healthy; chưa kiểm thử CDC/task failover | M2 |
| 2026-11-08 | Thêm `kafka/topics.json`, `setup_topics.py` và unit test; tạo năm topic CDC/DLQ RF=3, partitions=3, min ISR=2, retention bảy ngày; chạy lại không tạo thêm, `--check` đạt và thử cấu hình lệch không làm thay đổi topic | M2 |
| 2026-11-08 | Thêm Kafbat UI `v1.5.0` pin digest, bind loopback `8084`, chỉ đọc, tắt dynamic config/MCP; HTTP/API đọc đạt, mười yêu cầu ghi bị từ chối, topic manifest vẫn khớp; chưa kiểm tra tương tác/render trực tiếp trên trình duyệt | M2 |

## Quy ước trạng thái

- `TODO`: chưa bắt đầu.
- `IN_PROGRESS`: đang thực hiện.
- `BLOCKED`: bị chặn, ghi rõ nguyên nhân trong ghi chú.
- `DONE`: đã có code và kiểm thử đạt.
