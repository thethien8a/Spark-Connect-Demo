# Tracking Tiến Độ

> Cập nhật file này sau mỗi phiên làm việc. Đây là nguồn duy nhất để biết project đang ở đâu.

## Trạng thái hiện tại

| Mục | Giá trị |
| --- | --- |
| Trạng thái tổng thể | Đang chuẩn bị triển khai |
| Mốc hiện tại | M0 — chốt phạm vi và compatibility |
| Đã hoàn thành | Kế hoạch kiến trúc và thứ tự triển khai |
| Đang làm | Chưa bắt đầu code runtime |
| Bước tiếp theo | Chốt version, tạo PostgreSQL nguồn và generator |
| Cập nhật cuối | 2026-08-29 |

## Tiến độ theo mốc

| Mốc | Nội dung | Trạng thái | Ghi chú |
| --- | --- | --- | --- |
| M0 | Phạm vi và compatibility matrix | `IN_PROGRESS` | Use case đã chọn; chưa kiểm chứng version |
| M1 | PostgreSQL nguồn và generator | `TODO` | Chưa có schema/generator |
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

- [ ] PostgreSQL phát sinh INSERT/UPDATE/DELETE hợp lệ.
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

## Quy ước trạng thái

- `TODO`: chưa bắt đầu.
- `IN_PROGRESS`: đang thực hiện.
- `BLOCKED`: bị chặn, ghi rõ nguyên nhân trong ghi chú.
- `DONE`: đã có code và kiểm thử đạt.
