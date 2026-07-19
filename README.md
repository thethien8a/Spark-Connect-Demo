# Spark Connect Demo

Project demo xử lý dữ liệu bằng **Apache Spark 4.1.2** và **Spark Connect**. Cụm Spark chạy bằng Docker Compose, gồm 1 master, 2 worker và 1 Spark Connect server.

## Yêu cầu

- Docker và Docker Compose
- Python 3

## Cách chạy

### 1. Khởi động Spark

```bash
docker compose up -d
```

### 2. Cài thư viện Python

```bash
python -m venv venv
```

Kích hoạt môi trường ảo trên Windows:

```bash
venv\Scripts\activate
```

Sau đó cài dependencies:

```bash
pip install -r requirements.txt
```

### 3. Chạy notebook

Mở file `test.ipynb` bằng VS Code hoặc Jupyter và chạy cell trong notebook.

Notebook kết nối đến Spark Connect tại `sc://localhost:15002` và đọc dữ liệu từ `data/orders.csv`.

## Giao diện quản lý

- Spark Master: http://localhost:8080
- Worker 1: http://localhost:8081
- Worker 2: http://localhost:8082
- Spark Application: http://localhost:4040

## Dừng project

```bash
docker compose down
```
