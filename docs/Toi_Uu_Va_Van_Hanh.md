# Tối Ưu Và Vận Hành

## Mốc M7 — Conflict tests

Phân biệt:

- **Catalog commit conflict:** có thể retry có giới hạn.
- **Data conflict:** hai writer chạm dữ liệu chồng lấn; không retry mù quáng.

Nguyên tắc MVP:

1. Mỗi bảng Silver chỉ có một streaming writer.
2. Pipeline khác chỉ đọc hoặc ghi bảng riêng.
3. Đo conflict trước khi tăng retry.
4. Chỉ dùng snapshot isolation nếu runtime đã kiểm chứng.

Test hai append, hai update cùng khóa, update/delete cùng khóa và restart giữa các commit.

## Mốc M8 — COW và MOR

| Chiến lược | Ghi | Đọc | Hướng dùng |
| --- | --- | --- | --- |
| COW | Tạo lại file liên quan | Nhanh | Baseline, workload đọc nhiều |
| MOR | Ghi change/delete files | Có chi phí merge | Thử cho Silver CDC |

Benchmark cùng workload, trigger và tài nguyên; đo micro-batch latency, query latency, dung lượng, small files, delete files, retry và conflict.

Không khẳng định `MERGE INTO` tạo equality delete nếu chưa kiểm tra metadata và kết quả đọc thực tế.

## Mốc M8 — Airflow maintenance

```text
health check
→ rewrite data files
→ rewrite manifests
→ expire snapshots
→ remove orphan files
→ quality report
```

Quy tắc:

- Chạy ngoài giờ cao điểm và tránh MERGE/rewrite đang chạy.
- Giữ snapshot đủ cho audit/time travel, ví dụ 7 ngày ở demo.
- Orphan cleanup phải có safety window lớn hơn thời gian commit tối đa.
- Ghi số file/dung lượng trước và sau maintenance.
- Không bật xóa metadata tự động trước khi chốt retention.

## Mốc M9 — Tính năng nâng cao

### Schema evolution

Thêm `campaign_id`, cập nhật CDC/parser, ghi dữ liệu mới và xác nhận dữ liệu cũ đọc `NULL`.

### Partition evolution

Bắt đầu partition theo ngày tạo đơn. Chỉ đổi partition khi workload cần; kiểm tra truy vấn qua cả partition spec cũ và mới.

### Time travel

Dùng snapshot để điều tra lỗi, so sánh trước/sau commit và recovery trong retention window.

Multi-engine query bằng Trino, PyArrow hoặc PyIceberg là phần mở rộng sau MVP.

## Observability và failure drill

Theo dõi:

- Kafka consumer lag.
- Input và rejected rows.
- Insert/update/delete count.
- Batch duration và commit time.
- Snapshot, data file và delete file count.
- DLQ và thời điểm maintenance gần nhất.

Thử dừng Spark, Kafka, MinIO và Debezium; gửi message sai schema; sau đó kiểm tra recovery và đối chiếu PostgreSQL nguồn.

## Điều kiện nghiệm thu

- Toàn bộ service chạy với version đã pin và volume bền vững.
- CDC insert/update/delete đúng.
- Bronze audit được và Silver current state đúng.
- Restart/replay/conflict có kết quả và log rõ ràng.
- Gold metric đối chiếu được.
- Maintenance không đổi kết quả truy vấn.
- Time travel và schema evolution được demo.
