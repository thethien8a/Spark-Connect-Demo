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

Nếu chưa đăng ký connector, `GET /connectors` trả `[]` là bình thường. Worker healthy và plugin được nhận diện chưa chứng minh CDC hoặc task failover; các kiểm thử đó thực hiện sau khi đăng ký PostgreSQL connector.

### Chạy HAProxy cho Connect REST API

Service `connect-lb` trong `kafka/compose.yaml` dùng image `haproxy:3.4.4-alpine3.24` và mount read-only file `kafka/haproxy/haproxy.cfg`. Không cần build lại Kafka Connect image khi thay đổi cấu hình proxy.

HAProxy chia request bằng round-robin, kiểm tra `GET /` trên từng worker mỗi `2s` và chỉ nhận HTTP `200`. Worker bị loại sau ba lần kiểm tra lỗi liên tiếp và được nhận lại sau hai lần thành công. Docker DNS resolver cập nhật địa chỉ backend khi container worker được tạo lại.

Chỉ proxy publish REST port tại `127.0.0.1:8083`; hai worker vẫn chỉ mở `8083` trong Docker network. Endpoint không có authentication nên không publish ra LAN. Health check của `connect-lb` gọi qua proxy và chỉ đạt khi có ít nhất một worker phục vụ được request.

Kiểm tra cấu hình và khởi động từ thư mục gốc project:

```powershell
docker compose --env-file .env -f compose.yaml config --quiet
docker compose --env-file .env -f compose.yaml run --rm --no-deps connect-lb haproxy -c -f /usr/local/etc/haproxy/haproxy.cfg
docker compose --env-file .env -f compose.yaml up -d --wait --wait-timeout 240 connect-lb
```

Lệnh `up` chờ cả hai worker healthy khi khởi động ban đầu và khởi động Kafka dependency nếu cần; không khởi động Spark hoặc PostgreSQL. Khi đã chạy, HAProxy tiếp tục phục vụ qua worker còn lại nếu một worker dừng.

Client quản trị connector dùng một endpoint duy nhất:

```powershell
Invoke-RestMethod http://localhost:8083/
Invoke-RestMethod http://localhost:8083/connector-plugins
Invoke-RestMethod http://localhost:8083/connectors
docker compose --env-file .env -f compose.yaml logs --tail 40 connect-lb
```

HTTP access log ghi rõ backend `connect_workers/connect-1` hoặc `connect_workers/connect-2` để xác nhận routing, không cần thêm header riêng vào Connect REST response. Sau khi sửa `haproxy.cfg`, chạy lại bước kiểm tra cấu hình rồi `docker compose --env-file .env -f compose.yaml restart connect-lb` để nạp cấu hình mới.

Kiểm thử dừng từng worker sẽ gây Connect rebalance; chỉ chạy khi chấp nhận tác động này. Ví dụ kiểm tra REST failover, có `finally` để khởi động lại worker nếu request lỗi:

```powershell
foreach ($worker in "connect-1", "connect-2") {
    try {
        docker compose --env-file .env -f compose.yaml stop $worker
        Start-Sleep -Seconds 8
        1..6 | ForEach-Object {
            $response = Invoke-WebRequest -UseBasicParsing -TimeoutSec 10 -Uri "http://localhost:8083/?probe=${worker}-down-$_"
            if ($response.StatusCode -ne 200) { throw "Connect REST request failed" }
            $response.StatusCode
        }
    } finally {
        docker compose --env-file .env -f compose.yaml up -d --no-deps --wait --wait-timeout 60 $worker
    }
}
docker compose --env-file .env -f compose.yaml logs --tail 50 connect-lb
```

**Kết quả kiểm thử HAProxy ngày 2026-11-08: đạt.**

- `GET /`, `/connector-plugins` và `/connectors` qua endpoint chung thành công; plugin Debezium PostgreSQL là `3.6.1.Final`.
- Access log xác nhận request được phục vụ bởi cả hai worker khi chúng healthy.
- Khi dừng `connect-1`, cả sáu request thử nghiệm trả HTTP `200` qua `connect-2`; khi dừng `connect-2`, kết quả tương tự qua `connect-1`.
- Khi cả hai worker dừng và bị loại khỏi backend, endpoint trả HTTP `503`.
- Sau khi khởi động lại, hai worker và HAProxy đều healthy, request lại được chia tới cả hai; port của worker không publish ra host.

Các bài kiểm thử này dùng connector registry rỗng, không chứng minh CDC hoặc connector task failover. Topic CDC được quản lý ở mục dưới; connector vẫn chưa đăng ký. Seed/generator vẫn là script riêng, không tự chạy trong Compose.

### Quản lý topic CDC bằng manifest

`kafka/topics.json` là cấu hình có version cho các topic nghiệp vụ. `kafka/setup_topics.py` đọc manifest và dùng Kafka CLI trong container để tạo topic còn thiếu hoặc kiểm tra topic đã có. Script chỉ dùng thư viện chuẩn Python, không thêm dependency và không tự chạy trong Compose.

Quy ước tên topic cho connector PostgreSQL sắp triển khai là `topic.prefix=ecommerce`, schema `public`:

| Topic | Vai trò |
| --- | --- |
| `ecommerce.public.customers` | CDC bảng `public.customers` |
| `ecommerce.public.products` | CDC bảng `public.products` |
| `ecommerce.public.orders` | CDC bảng `public.orders` |
| `ecommerce.public.order_items` | CDC bảng `public.order_items` |
| `ecommerce.dlq` | Chuẩn bị cho luồng xử lý lỗi; hiện chưa có thành phần ghi vào topic này |

Mỗi topic khai báo rõ `partitions=3`, `replication_factor=3`, `min.insync.replicas=2`, `retention.ms=604800000` (bảy ngày) và `cleanup.policy=delete`. Đây là chính sách hiện tại của project, không phải bộ thông số áp dụng cho mọi workload production. `delete` giữ lịch sử trong khoảng retention thay vì chỉ giữ trạng thái cuối theo key bằng compaction.

Điều kiện chạy: Docker CLI dùng được, broker đang chạy và có Python; có thể dùng Python trong `venv` hiện có. Từ thư mục gốc project, tạo topic còn thiếu và kiểm tra:

```powershell
.\venv\Scripts\python.exe -B .\kafka\setup_topics.py
```

Chế độ chỉ đọc, phù hợp cho kiểm tra trước/sau triển khai hoặc đưa vào CI:

```powershell
.\venv\Scripts\python.exe -B .\kafka\setup_topics.py --check
```

- Exit `0`: tất cả topic khai báo khớp manifest.
- Exit `1`: thiếu topic ở chế độ `--check`, cấu hình lệch, manifest không hợp lệ, hoặc lỗi Docker/Kafka/timeout. Lỗi tham số dòng lệnh do `argparse` trả exit `2`.
- Khi chưa có topic, `--check` báo `MISSING` và exit `1` là bình thường; lệnh này không tạo topic.

Hành vi an toàn khi chạy lại:

- Toàn bộ manifest được kiểm tra trước khi gọi Docker; từ chối tên trùng, trường JSON trùng và giá trị cấu hình có khoảng trắng ở đầu/cuối.
- Kiểm tra tất cả topic đã tồn tại trước khi tạo bất kỳ topic còn thiếu nào. Nếu có cấu hình lệch, in `DRIFT` với giá trị mong muốn/thực tế và dừng, không tạo hoặc sửa topic.
- So sánh partitions, số replica từng partition và các khóa `config` được khai báo trong manifest. Dùng `kafka-configs.sh --describe --all` để đọc giá trị hiệu lực, kể cả giá trị kế thừa từ broker; không chỉ kiểm tra override trên topic.
- Nếu replica đang được thêm/bớt trong một reassignment, báo lỗi để chạy lại sau khi thao tác đó hoàn tất, tránh coi trạng thái chuyển tiếp là đã khớp.
- Chỉ tạo topic còn thiếu bằng `--if-not-exists`, rồi đọc lại để xác minh. Nếu một tiến trình khác vừa tạo topic cùng tên nhưng khác cấu hình, script vẫn báo lệch thay vì mặc định coi là thành công.
- Không tự alter partitions/replication/config, không xóa hoặc tạo lại topic, không tác động topic ngoài manifest và không đọc credentials từ `.env`.

Script là bootstrap/validator, không phải công cụ tự áp dụng mọi thay đổi cấu hình. Nếu cần đổi một topic đang có, phải review và thực hiện thay đổi có chủ đích, rồi chạy `--check` lại. Việc tạo nhiều topic không phải transaction: nếu Kafka lỗi giữa chừng, topic đã tạo vẫn được giữ nguyên và có thể chạy lại script sau khi xử lý lỗi. Script cũng không thay thế kiểm tra leader/ISR, dữ liệu CDC hoặc consumer lag.

Manifest mặc định được tìm theo vị trí file script, không phụ thuộc thư mục đang đứng. Có thể chọn manifest khác hoặc dùng broker khác để chạy Kafka CLI:

```powershell
.\venv\Scripts\python.exe -B .\kafka\setup_topics.py --check --manifest .\kafka\topics.json --container kafka-2
```

`localhost:9092` ở đây là listener bên trong container được chọn, không phải listener trên Windows host. Cấu hình và parser CLI đã được kiểm tra với image Kafka `4.3.0` đang pin; khi nâng Kafka cần chạy lại test và smoke test CLI.

Chạy unit test, không cần Docker/Kafka:

```powershell
.\venv\Scripts\python.exe -B -m unittest discover -s kafka -p test_setup_topics.py -v
```

**Kết quả kiểm thử ngày 2026-11-08: đạt.**

- `--check` trước khi tạo báo đủ năm topic còn thiếu và không tạo topic.
- Lệnh setup tạo và xác minh đủ năm topic; chạy lại có `0 creation request(s)`; `--check` sau đó đạt.
- Manifest thử nghiệm đặt sai retention của `orders` đồng thời khai báo một topic còn thiếu: script exit `1` trước khi tạo topic còn thiếu, không thay đổi retention thực tế. Manifest thử nghiệm được đặt ngoài repo và đã dọn sau kiểm thử.
- Kiểm tra trực tiếp xác nhận mỗi topic có ba partition, RF=3, đúng cấu hình và mỗi partition có đủ ba replica trong ISR.
- Chạy bản script cuối từ thư mục khác và qua `kafka-2` vẫn tìm đúng manifest mặc định, kiểm tra thành công mà không tạo topic.

Bước tiếp theo là chuẩn bị dữ liệu nguồn để kiểm thử snapshot và đăng ký PostgreSQL connector qua HAProxy với `topic.prefix=ecommerce`. Các topic vừa tạo chưa chứng minh CDC hoặc task failover.

### Kafka web UI chỉ đọc

Service `kafka-ui` trong `kafka/compose.yaml` dùng Kafbat UI `v1.5.0`, pin cả version và image digest. Đây là giao diện vận hành Kafka, không phải dashboard nghiệp vụ. UI không thay thế `kafka/topics.json` và `kafka/setup_topics.py` làm nguồn cấu hình topic được quản lý trong Git.

- Endpoint mặc định: `http://localhost:8084`, chỉ publish trên `127.0.0.1`; không trùng Spark Master UI tại host port `8080` hoặc Connect REST tại `8083`.
- Đổi host port bằng `KAFKA_UI_HOST_PORT` trong `.env`; `.env.example` khai báo `8084`. Nếu biến chưa có trong `.env`, Compose vẫn dùng mặc định `8084`.
- Cluster hiển thị là `spark-connect-lab`, kết nối trực tiếp tới `kafka-1:9092,kafka-2:9092,kafka-3:9092` qua network `platform`.
- Mục Kafka Connect hiển thị instance `debezium`, đọc trạng thái qua `http://connect-lb:8083`, không gọi trực tiếp hai worker và không dùng `localhost` của Windows từ container.
- Chạy bằng user `kafkaui` trong image; JVM heap `128-512 MB`, container memory limit `1 GiB`. UI không mount volume Kafka, Docker socket hoặc credentials từ `.env`.

Các bảo vệ được khai báo rõ trong Compose:

- `KAFKA_CLUSTERS_0_READONLY=true`: backend từ chối thao tác thay đổi topic, message, consumer group và connector; không chỉ ẩn nút trên frontend.
- `DYNAMIC_CONFIG_ENABLED=false`: không cho đổi cấu hình cluster hoặc bỏ chế độ chỉ đọc bằng giao diện/API cấu hình động.
- `MCP_ENABLED=false`: không bật MCP server của Kafbat UI, tránh có thêm đường thực thi ngoài REST API của cluster.
- `HTTP_ERROR_EXCLUDESTACKTRACES=true`: response lỗi không trả đầy đủ stack trace.
- `AUTH_TYPE=DISABLED`: không có màn hình đăng nhập. Chỉ dùng trong local/lab; tiến trình trên host hoặc container trong shared network vẫn có thể truy cập UI và đọc dữ liệu.

Chế độ chỉ đọc của ứng dụng không phải Kafka ACL và không bảo vệ các kết nối trực tiếp tới broker hoặc Connect REST. Không đổi binding sang LAN/public nếu chưa thiết kế authentication, authorization và TLS phù hợp.

Từ thư mục gốc, kiểm tra cấu hình rồi khởi động UI:

```powershell
docker compose --env-file .env -f compose.yaml config --quiet
docker compose --env-file .env -f compose.yaml up -d --wait --wait-timeout 180 kafka-ui
```

Lệnh `up` bình thường chờ ba broker và `connect-lb` healthy trong lần khởi động ban đầu. Nếu một dependency đang lỗi, lệnh này có thể chưa khởi động UI dù broker khác vẫn truy cập được. Khi dependency đã được vận hành riêng và cần khởi động/recreate chỉ UI, dùng `up -d --no-deps --wait --wait-timeout 180 kafka-ui`. Docker không tự dừng UI chỉ vì dependency chuyển unhealthy sau khi UI đã chạy.

Health check dùng BusyBox `wget` có sẵn trong image để gọi `http://127.0.0.1:8080/actuator/health`. Đây là health của ứng dụng UI; cần kiểm tra thêm cluster `ONLINE` trong giao diện hoặc API, không coi UI healthy là đã chứng minh CDC hoạt động.

Kiểm tra từ PowerShell, với host port mặc định:

```powershell
Invoke-RestMethod http://localhost:8084/actuator/health
Invoke-RestMethod http://localhost:8084/api/clusters
Invoke-RestMethod http://localhost:8084/api/clusters/spark-connect-lab/brokers
Invoke-RestMethod http://localhost:8084/api/clusters/spark-connect-lab/connects
docker compose --env-file .env -f compose.yaml logs --tail 50 kafka-ui
.\venv\Scripts\python.exe -B .\kafka\setup_topics.py --check
```

Sau khi mở UI, chọn cluster `spark-connect-lab`:

1. **Brokers**: xem ba node, leader và replica/ISR.
2. **Topics**: tìm `ecommerce.` để xem bốn topic CDC và DLQ, partitions và cấu hình; mở topic để xem message.
3. **Consumer Groups**: xem group, trạng thái và lag khi có consumer phù hợp.
4. **Kafka Connect**: xem instance `debezium`, plugin và trạng thái connector khi đã đăng ký.

Bước bổ sung UI này chưa chạy seed/generator hoặc đăng ký connector CDC, nên danh sách connector/consumer group và message nghiệp vụ có thể rỗng khi chưa có luồng dữ liệu. Chưa cấu hình JMX/Prometheus cho UI; các biểu đồ throughput và Kafka version trong UI có thể trống/`Unknown`, dù metadata broker/topic vẫn đọc được. Không thay cấu hình broker hoặc dựng metrics stack trong bước bổ sung UI này.

**Kết quả kiểm thử ngày 2026-11-08: đạt ở mức HTTP/API.**

- Frontend HTML cùng bundle JavaScript/CSS trả HTTP `200`; actuator trả `UP`, cluster trả `ONLINE`, `readOnly=true` và đủ ba broker.
- Cả năm topic nghiệp vụ có ba partition, RF=3, chín replica in-sync trên mỗi topic và không có partition under-replicated. Đọc message qua API SSE thành công.
- Đọc consumer group, Connect instance qua HAProxy và plugin Debezium PostgreSQL thành công; chưa có connector đăng ký.
- Mười yêu cầu ghi thử nghiệm thuộc topic/message/group/Connect đều bị backend từ chối với HTTP `405`, mã lỗi `4003`, thông báo `This cluster is in read-only mode.`. Tên topic/group/connector dùng để thử xóa hoặc thay đổi là tên thử nghiệm không tồn tại; không xóa hay sửa tài nguyên thật.
- Đổi dynamic config bị từ chối với HTTP `400`, mã `4002`; MCP routes không hoạt động; stack trace đã được che.
- Danh sách topic và connector không thay đổi sau thử nghiệm; `setup_topics.py --check` và 20 unit test quản lý topic vẫn đạt.
- Chưa xác nhận tương tác/render trên Chrome hoặc layout desktop/mobile: kiểm tra tự động lần này chỉ xác minh HTTP/API và tải bundle frontend.

Tham khảo: [Kafbat UI v1.5.0](https://github.com/kafbat/kafka-ui/releases/tag/v1.5.0), [read-only filter đúng version](https://github.com/kafbat/kafka-ui/blob/v1.5.0/api/src/main/java/io/kafbat/ui/config/ReadOnlyModeFilter.java).

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
kafka-ui (Kafbat, local read-only)
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
