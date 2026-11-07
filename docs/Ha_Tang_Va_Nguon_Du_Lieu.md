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
3. Tạo bốn bảng nguồn và primary key.
4. Viết seed script.
5. Viết generator tạo order và cập nhật trạng thái theo transition hợp lệ.
6. Tạo các tình huống INSERT, UPDATE, DELETE và dữ liệu lỗi có kiểm soát.

**Đạt khi:** PostgreSQL có dữ liệu thay đổi liên tục, generator chạy lại được và có thể dừng mà không làm hỏng dữ liệu.

### Chạy M1 local

Từ thư mục gốc project, tạo `.env` từ cấu hình mẫu rồi chạy M1:

```powershell
Copy-Item .env.example .env
docker compose --env-file .env -f compose.yaml up -d source-postgres
venv\Scripts\python.exe source-postgres\generator.py seed
venv\Scripts\python.exe source-postgres\generator.py smoke
venv\Scripts\python.exe source-postgres\generator.py run
```

`source-postgres/config.py` nạp `.env` và dựng cấu hình kết nối `source_app` từ các biến riêng, không lưu DSN có password. Cấu hình Debezium dùng hostname nội bộ `SOURCE_POSTGRES_INTERNAL_HOST`; connector M2 sẽ lấy cùng nhóm biến `DEBEZIUM_*`.

`smoke` kiểm tra INSERT, toàn bộ transition hợp lệ, transition sai bị từ chối và DELETE cascade. `run` phát sinh thay đổi liên tục; dừng bằng `Ctrl+C`, transaction đang dở sẽ được rollback khi kết nối đóng.

Các script trong `source-postgres/init` chỉ chạy khi volume PostgreSQL được khởi tạo lần đầu. Khi thay đổi schema hoặc role, cần chủ động tạo lại volume development trước khi kiểm thử lại.

## Mốc M2 — Kafka và Debezium

### Quyết định triển khai M2

M2 dùng ba Kafka node chạy KRaft ở combined broker/controller mode, hai Kafka Connect worker chạy distributed mode và một HAProxy làm endpoint chung cho Connect REST API. Đây là topology production-like cơ bản: có quorum, replication, persistent state, worker failover và health-aware routing, nhưng vẫn đủ gọn để quan sát từng thành phần trên một máy học tập.

| Thành phần | Phiên bản/image đã pin | Ghi chú |
| --- | --- | --- |
| Kafka | `apache/kafka:4.3.0` | Image JVM chính thức, ba broker/controller KRaft |
| Kafka Connect | Custom image dựa trên `apache/kafka:4.3.0` | Hai worker dùng chung group và internal topics |
| Debezium PostgreSQL connector | `3.6.1.Final` | Plugin archive được pin version và kiểm tra SHA-512 khi build |
| Kafka Connect load balancer | `haproxy:3.4.4-alpine3.24` | Một endpoint REST, HTTP health check cho hai worker |
| PostgreSQL nguồn | `postgres:17.6-bookworm` | Đã bật logical replication ở M1 |

Không dùng image `quay.io/debezium/kafka` hoặc `quay.io/debezium/connect` vì Debezium xác định chúng dành cho test/evaluation. Connect image được build rõ ràng từ Kafka chính thức và chỉ cài PostgreSQL connector lấy từ Maven Central. Cách này gần quy trình production hơn: artifact có version, checksum và image có thể tái tạo.

Topology và kết nối dự kiến:

- `kafka-1`, `kafka-2`, `kafka-3` đều giữ broker và controller role; metadata quorum chịu được một node lỗi.
- Mỗi broker có persistent volume riêng, replication factor mặc định `3` và `min.insync.replicas=2`.
- Container bootstrap qua `kafka-1:9092,kafka-2:9092,kafka-3:9092`.
- Client trên host dùng ba listener tại `localhost:29092`, `localhost:39092`, `localhost:49092`.
- `connect-1` và `connect-2` dùng chung Connect group; config, offset và status topics có replication factor `3`.
- Mỗi Connect worker advertise hostname nội bộ riêng để request có thể được forward đúng giữa các worker.
- HAProxy kiểm tra `GET /` trên `connect-1:8083` và `connect-2:8083`, chỉ route tới worker trả HTTP `200`.
- Client quản trị Connect dùng endpoint duy nhất `http://localhost:8083`; REST port của từng worker không publish trực tiếp ra host.
- Kafka, Kafka Connect và `source-postgres` cùng dùng network `common-network`.

Combined mode giúp học quorum và broker replication mà không phải vận hành sáu Kafka process. Production quan trọng nên tách tối thiểu ba controller khỏi các broker để có thể scale và rolling update độc lập. M2 dùng listener tách riêng cho broker, controller và host client nhưng giữ giao thức `PLAINTEXT` trong local lab; TLS, SASL và ACL là bước hardening bắt buộc trước khi triển khai trên hạ tầng thật.

HAProxy chỉ cân bằng Kafka Connect REST API dùng để đăng ký, cập nhật và quan sát connector. Kafka producer/consumer không đi qua HAProxy vì sau bước bootstrap chúng tự khám phá broker và kết nối trực tiếp tới partition leader. Một HAProxy đủ để học routing và worker failover nhưng bản thân nó vẫn là single point of failure; production thật cần nhiều load balancer instance hoặc endpoint HA do Kubernetes/cloud cung cấp.

Nguồn tham chiếu:

- [Apache Kafka Docker image](https://kafka.apache.org/43/getting-started/docker/)
- [Apache Kafka KRaft](https://kafka.apache.org/43/operations/kraft/)
- [Kafka Connect REST API](https://github.com/apache/kafka/blob/trunk/docs/kafka-connect/user-guide.md)
- [Cài Debezium connector plugin](https://debezium.io/documentation/reference/3.6/install.html)
- [Debezium PostgreSQL connector](https://debezium.io/documentation/reference/3.6/connectors/postgresql.html)
- [HAProxy configuration](https://github.com/haproxy/docs/blob/master/_autodocs/01-configuration.md)
- [HAProxy official image](https://hub.docker.com/_/haproxy)
- [Docker Compose startup order và service health](https://docs.docker.com/compose/how-tos/startup-order)
- [Docker Compose image pull policy](https://github.com/docker/docs/blob/main/content/reference/compose-file/services.md#pull_policy)

1. Dựng ba Kafka node KRaft và persistent volume riêng.
2. Build custom image rồi dựng hai Kafka Connect worker có Debezium PostgreSQL connector.
3. Dựng HAProxy với round-robin routing và HTTP health check cho Connect REST API.
4. Đăng ký PostgreSQL connector qua endpoint HAProxy bằng cấu hình ngoài source code.
5. Tạo topic CDC và DLQ với replication factor `3`.
6. Dùng primary key làm Kafka key và cấu hình retention đủ cho replay.
7. Kiểm tra `before`, `after`, `op`, source metadata, offset và Connect worker failover.

**Đạt khi:** một thao tác insert/update/delete trong PostgreSQL xuất hiện đúng ở topic tương ứng, có thể đọc lại và Connect REST API vẫn dùng được qua HAProxy khi dừng một worker.

### Vận hành Kafka KRaft cluster

Ba Kafka volume được khai báo `external` để Compose không quản lý việc tạo/xóa dữ liệu. Nếu dùng Docker Engine mới chưa có các volume này, tạo một lần trước khi khởi động:

```powershell
docker volume create spark-connect-lab_kafka-1-data
docker volume create spark-connect-lab_kafka-2-data
docker volume create spark-connect-lab_kafka-3-data
```

Không xóa volume để chạy lại worker; trạng thái Connect được lưu trong Kafka internal topics.

Khởi động riêng ba Kafka node từ thư mục gốc project:

```powershell
docker compose --env-file .env -f kafka\compose.yaml up -d kafka-1 kafka-2 kafka-3
docker compose --env-file .env -f kafka\compose.yaml ps
```

Ba node dùng chung `KAFKA_CLUSTER_ID` nhưng có `KAFKA_NODE_ID`, advertised listener và persistent volume riêng. Listener nội bộ `9092` dành cho broker/client trong Docker, listener `9093` dành cho controller quorum và listener `9094` được map ra ba host port khác nhau. Broker không tự tạo topic; topic nghiệp vụ sẽ được khai báo chủ động với replication factor `3` ở bước cấu hình CDC.

Kiểm tra trạng thái metadata quorum khi cần:

```powershell
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh `
  --bootstrap-server localhost:9092 describe --status
```

### Build và kiểm tra Kafka Connect image

Custom image trong `kafka/connect` dùng `apache/kafka:4.3.0`, tải đúng Debezium PostgreSQL connector `3.6.1.Final` từ Maven Central và kiểm tra SHA-512 trước khi giải nén. Build sẽ dừng ngay nếu artifact không khớp checksum đã pin.

Build image từ thư mục gốc project:

```powershell
docker build `
  --tag spark-connect-lab/kafka-connect:4.3.0-debezium-3.6.1 `
  kafka\connect
```

Khi cần smoke test độc lập trước khi thêm worker vào Compose, chạy một worker tạm trên shared network:

```powershell
docker run --rm --name connect-plugin-smoke `
  --hostname connect-plugin-smoke `
  --network common-network `
  -p 18083:8083 `
  spark-connect-lab/kafka-connect:4.3.0-debezium-3.6.1
```

Từ terminal khác, kiểm tra plugin:

```powershell
Invoke-RestMethod http://localhost:18083/connector-plugins
```

Kết quả phải chứa class `io.debezium.connector.postgresql.PostgresConnector` với version `3.6.1.Final`. Worker dùng `plugin.path=/opt/kafka/plugins`, tự advertise Docker hostname nếu `CONNECT_REST_ADVERTISED_HOST_NAME` không được cung cấp và tạo ba internal topic compacted với replication factor `3`.

### Chạy hai Kafka Connect worker distributed

`kafka/compose.yaml` định nghĩa `connect-1` và `connect-2`, dùng chung custom image đã build ở bước trên. `pull_policy: never` yêu cầu image có sẵn trên Docker Engine, không thử tải custom image từ registry. Sau khi thay đổi Dockerfile, plugin hoặc cấu hình đóng gói trong image, build lại image trước khi chạy Compose.

Hai worker dùng chung `group.id=spark-connect-lab-connect` và ba internal topic, nhưng advertise hostname riêng qua `CONNECT_REST_ADVERTISED_HOST_NAME`. Mỗi JVM có heap tối đa `512 MB`. Worker chỉ khởi động sau khi cả ba broker healthy; health check của worker gọi `GET /` tại `localhost:8083` bằng `wget` có sẵn trong image.

Khởi động từ thư mục gốc project sau khi tạo các Kafka volume và build image:

```powershell
docker compose --env-file .env -f compose.yaml config --quiet
docker compose --env-file .env -f compose.yaml up -d --wait --wait-timeout 240 connect-1 connect-2
docker compose --env-file .env -f compose.yaml ps
```

Lệnh `up` chỉ khởi động hai worker cùng ba Kafka dependency, không khởi động Spark hoặc PostgreSQL. Có thể dùng `-f kafka\compose.yaml` khi chỉ vận hành module Kafka.

REST port của worker không publish trực tiếp ra host. Kiểm tra API bên trong container:

```powershell
foreach ($worker in "connect-1", "connect-2") {
  docker exec $worker wget -q -O - http://localhost:8083/
  docker exec $worker wget -q -O - http://localhost:8083/connector-plugins
  docker exec $worker wget -q -O - http://localhost:8083/connectors
}
```

Kiểm tra hostname REST có thể được worker còn lại truy cập:

```powershell
docker exec connect-1 wget -q -O - http://connect-2:8083/
docker exec connect-2 wget -q -O - http://connect-1:8083/
docker compose --env-file .env -f compose.yaml logs --tail 100 connect-1 connect-2
```

Kiểm tra partition, replication factor và compaction của ba internal topic:

```powershell
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic "spark-connect-lab-connect-(configs|offsets|status)"
```

**Smoke test đạt khi:**

- Ba broker và hai worker đều healthy.
- `GET /` trên hai worker trả cùng Kafka cluster ID và version Kafka `4.3.0`.
- `GET /connector-plugins` trên từng worker đều có Debezium PostgreSQL `3.6.1.Final`.
- Hai worker truy cập được REST API của nhau bằng hostname nội bộ.
- Log xác nhận hai worker tham gia cùng Connect group, có assignment thành công và ổn định sau rebalance.
- Ba internal topic `spark-connect-lab-connect-configs`, `spark-connect-lab-connect-offsets`, `spark-connect-lab-connect-status` có replication factor `3` và `cleanup.policy=compact`; số partition lần lượt là `1`, `25`, `5`.

Nếu chưa đăng ký connector, `GET /connectors` trả `[]` là bình thường. Worker healthy và plugin được nhận diện chưa chứng minh CDC hoặc task failover; các kiểm thử đó thực hiện sau khi dựng HAProxy và đăng ký PostgreSQL connector.

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
haproxy cho kafka-connect REST API
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

### Tách service và kết nối mạng

Compose root `compose.yaml` chỉ làm nhiệm vụ ghép các service. Định nghĩa từng service nằm trong folder tương ứng:

```text
source-postgres/compose.yaml
kafka/compose.yaml
spark/docker-compose.yaml
```

`spark/docker-compose.yaml` là một module duy nhất chứa toàn bộ Spark cluster: master, hai worker và Spark Connect. Các hạ tầng độc lập như PostgreSQL, Kafka, MinIO và Airflow sẽ có Compose file riêng ở folder của chúng.

Root và các module dùng network `common-network`. Docker DNS cho phép gọi nhau bằng service name, ví dụ `spark-master:7077`, `source-postgres:5432` hoặc `kafka:9092`; không dùng IP container cố định.

Khởi động toàn bộ stack:

```powershell
docker compose --env-file .env -f compose.yaml up -d
```

Có thể khởi động riêng nhóm Spark hoặc PostgreSQL khi cần; cả hai vẫn dùng cùng network cố định:

```powershell
docker compose --env-file .env -f source-postgres\compose.yaml up -d
docker compose -f spark\docker-compose.yaml up -d
```

Trong cách chạy riêng, khởi động PostgreSQL và Spark từ hai lệnh trên cùng project directory; khi chạy toàn bộ nên dùng root để Compose quản lý một lifecycle thống nhất.

Smoke test Spark Connect:

```python
spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
```

Từ host dùng địa chỉ publish tương ứng; từ container dùng service name nội bộ.
