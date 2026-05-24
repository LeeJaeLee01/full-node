# Kafka Single Node - Khái niệm, Triển khai và Sử dụng

Tài liệu này mô tả cách cài đặt Apache Kafka single node bằng Docker và triển khai tích hợp Produce/Consume message cơ bản trong NestJS.

---

## 1. Kafka là gì?

**Apache Kafka** là một nền tảng streaming phân tán (distributed event streaming platform), dùng để:

- Publish và subscribe message theo luồng thời gian thực
- Lưu trữ stream dữ liệu một cách bền vững (durable)
- Xử lý stream dữ liệu theo batch hoặc real-time

Kafka thường được dùng cho: event-driven architecture, log aggregation, message queue, data pipeline, microservices communication.

---

## 2. Các khái niệm cốt lõi

| Khái niệm | Mô tả |
|-----------|-------|
| **Broker** | Một server Kafka, nhận và lưu message |
| **Cluster** | Tập hợp nhiều broker (ở đây ta dùng **single node** = 1 broker) |
| **Topic** | Kênh/category để phân loại message (ví dụ: `demo-messages`) |
| **Partition** | Topic được chia thành các partition để scale song song |
| **Producer** | Ứng dụng gửi (publish) message vào topic |
| **Consumer** | Ứng dụng đọc (subscribe) message từ topic |
| **Consumer Group** | Nhóm consumer cùng chia sẻ việc đọc message; mỗi partition chỉ được 1 consumer trong group xử lý |
| **Offset** | Vị trí (index) của message trong partition |
| **KRaft** | Chế độ quản lý metadata mới của Kafka, **không cần Zookeeper** |

---

## 3. Kiến trúc trong project này

```
┌─────────────────────┐     ┌──────────────────────────┐
│  backend/ (API)     │     │  backend-consumer/       │
│  Kafka: init+connect│     │  Kafka: init+connect     │
│  Producer only      │     │  Consumer only           │
└──────────┬──────────┘     └────────────┬─────────────┘
           │ produce                      │ consume
           ▼                              ▼
      Kafka Broker (Docker, port 9092)
           │
           └──► Kafka UI (port 8080)
```

| Project | Script chạy | Kafka |
|---------|---------------|-------|
| `backend/` (consumer entry) | `npm run start:consumer:dev` | Connect + consume |
| `backend/scripts/` | `kafka:single:*` | Demo CLI |

**Luồng hoạt động:**

1. Producer gửi message vào topic `demo-messages`
2. Kafka broker lưu message vào partition
3. Consumer subscribe topic và nhận message theo consumer group

---

## 4. Triển khai Kafka Single Node bằng Docker

File cấu hình: [`docker-compose.yml`](../docker-compose.yml)

### 4.1. Khởi động Kafka

```bash
cd backend
npm run kafka:up
```

Hoặc:

```bash
docker compose up -d
```

Kafka sẽ chạy tại: `localhost:9092`

Kafka UI sẽ chạy tại: **http://localhost:8080**

### 4.2. Kafka UI (GUI)

Project đã tích hợp sẵn **[UI for Apache Kafka](https://github.com/provectus/kafka-ui)** (Provectus) — giao diện web giúp theo dõi Kafka trực quan, không cần dùng CLI.

#### Khởi động và truy cập

Sau khi chạy `npm run kafka:up`, mở trình duyệt:

```
http://localhost:8080
```

Kafka UI tự khởi động cùng Kafka broker (chờ broker healthy rồi mới start).

#### Cấu hình trong Docker

Service `kafka-ui` trong [`docker-compose.yml`](../docker-compose.yml):

| Cấu hình | Giá trị | Ý nghĩa |
|----------|---------|---------|
| Image | `provectuslabs/kafka-ui:latest` | Giao diện web quản lý Kafka |
| Port | `8080` | Truy cập từ máy host |
| Cluster name | `local` | Tên cluster hiển thị trên UI |
| Bootstrap servers | `kafka:29092` | Kết nối nội bộ Docker tới broker |

> **Lưu ý:** Kafka UI kết nối broker qua `kafka:29092` (mạng Docker), còn NestJS/script trên máy host dùng `localhost:9092`. Hai port khác nhau nhưng cùng trỏ tới 1 broker.

#### Các màn hình chính trên Kafka UI

**1. Brokers**

- Xem broker đang online/offline
- Kiểm tra Kafka cluster có sẵn sàng trước khi test produce/consume

**2. Topics**

- Danh sách topic (ví dụ: `demo-messages`)
- Số partition, replication factor
- Click vào topic → tab **Messages** để xem nội dung message
- Có thể produce message thử trực tiếp từ UI (tab **Produce Message**)

**3. Messages (trong từng topic)**

- Đọc message theo offset, partition, timestamp
- Filter / tìm kiếm message
- Hữu ích để xác nhận producer đã ghi message thành công

**4. Consumer Groups**

- Xem group `full-node-consumer-group` (NestJS consumer)
- Các cột quan trọng:

| Cột | Ý nghĩa |
|-----|---------|
| **CURRENT-OFFSET** | Offset consumer đã đọc tới |
| **LOG-END-OFFSET** | Offset cuối cùng của topic |
| **LAG** | Số message chưa được consume (`LOG-END - CURRENT`) |

- LAG = 0 nghĩa là consumer đã xử lý hết message

#### Workflow gợi ý để học Kafka qua UI

```
Bước 1: Mở http://localhost:8080
           │
Bước 2: Vào Topics → chọn "demo-messages"
           │         (topic tự tạo sau lần produce đầu tiên)
           │
Bước 3: Gửi message
           ├── npm run kafka:single:produce -- "Hello"
           └── (tuỳ chọn) NestJS API backend
           │
Bước 4: Quay lại Kafka UI → tab Messages → thấy message mới
           │
Bước 5: Vào Consumer Groups
           └── offset tăng sau khi chạy `npm run kafka:single:consume`
```

#### Script liên quan

```bash
npm run kafka:up        # Khởi động Kafka + Kafka UI
npm run kafka:down      # Dừng tất cả
npm run kafka:logs      # Log của broker
npm run kafka:ui:logs   # Log của Kafka UI
docker compose ps       # Kiểm tra cả 2 container đang chạy
```

#### Troubleshooting Kafka UI

| Vấn đề | Nguyên nhân | Cách xử lý |
|--------|-------------|------------|
| Không mở được `localhost:8080` | Container chưa start | `docker compose ps`, chạy lại `npm run kafka:up` |
| UI hiện "No clusters" / lỗi kết nối | Kafka broker chưa healthy | `npm run kafka:logs`, đợi broker healthy rồi restart UI |
| Không thấy topic | Chưa produce message lần nào | Gửi message qua API hoặc script produce |
| Thấy message nhưng LAG > 0 | Consumer chưa chạy | `npm run kafka:single:consume` hoặc `npm run dev:consumer` |
| Port 8080 bị chiếm | App khác dùng port | Đổi mapping port trong `docker-compose.yml` (ví dụ `8081:8080`) |

### 4.3. Kiểm tra trạng thái

```bash
docker compose ps
npm run kafka:logs
```

### 4.4. Dừng Kafka

```bash
npm run kafka:down
```

### 4.5. Cấu hình Docker quan trọng

- **Kafka image**: `apache/kafka:3.9.0`
- **Kafka UI image**: `provectuslabs/kafka-ui:latest`
- **Chế độ KRaft**: broker + controller trên cùng 1 node
- **Kafka port**: `9092` (kết nối từ máy host)
- **Kafka UI port**: `8080` (giao diện web)
- **Auto create topic**: bật (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`)
- **Volume**: `kafka-data` lưu dữ liệu persistent

---

## 5. Cấu hình môi trường

Copy file mẫu:

```bash
cp .env.example .env
```

| Biến môi trường | Mặc định | Ý nghĩa |
|-----------------|----------|---------|
| `KAFKA_BROKERS` | `localhost:9092` | Danh sách broker |
| `KAFKA_TOPIC` | `demo-messages` | Topic mặc định |
| `KAFKA_CLIENT_ID` | `full-node-backend` | ID client Kafka |
| `KAFKA_GROUP_ID` | `full-node-consumer-group` | Consumer group |

---

## 6. Cấu trúc project

```
full-node/
├── backend/
│   ├── src/
│   │   ├── main.ts              # API server
│   │   ├── kafka/               # Layer connect (dùng chung)
│   │   │   ├── kafka.constants.ts
│   │   │   ├── kafka.client.ts
│   │   │   └── kafka.module.ts
│   │   └── consumers/           # Logic consumer
│   │       ├── main.ts
│   │       ├── consumers.module.ts
│   │       └── kafka-consumer.service.ts
│   └── scripts/
```

| Entry | Script | Vai trò |
|-------|--------|---------|
| API | `npm run start:dev` | HTTP API (không gộp consumer) |
| Consumer service | `npm run start:consumer:dev` | Connect Kafka + consume |
| Demo CLI | `npm run kafka:single:*` | Script produce/consume |

---

## 7. Demo bằng Script (khuyên dùng)

**Không cần** chạy NestJS server hay consumer service.

### 7.1. Tạo topic

```bash
npm run kafka:single:setup
```

### 7.2. Produce message

```bash
npm run kafka:single:produce
npm run kafka:single:produce -- "Xin chao Kafka"
```

Alias cũ vẫn dùng được: `npm run kafka:produce`

### 7.3. Consume message

```bash
npm run kafka:single:consume
```

Alias: `npm run kafka:consume`. Nhấn `Ctrl+C` để dừng.

Chi tiết: [`scripts/single-node/README.md`](../scripts/single-node/README.md)

---

## 8. Hướng dẫn chạy end-to-end (Script)

```bash
cd backend

# 1. Kafka + Kafka UI
npm run kafka:up
# UI: http://localhost:8080

# 2. Tạo topic
npm run kafka:single:setup

# 3. Produce
npm run kafka:single:produce -- "Test Kafka message"

# 4. Consume (terminal khác)
npm run kafka:single:consume
```

| Nơi kiểm tra | Kết quả mong đợi |
|--------------|------------------|
| **Terminal consume** | `[single-node/consume] ... value="Test Kafka message"` |
| **Kafka UI → Topics → Messages** | Thấy message trong `demo-messages` |
| **Kafka UI → Consumer Groups** | Offset tăng sau khi consume |

### Tuỳ chọn: NestJS consumer

```bash
cd backend
npm run start:consumer:dev
# hoặc từ root: npm run dev:consumer
```

---

## 9. Consumer service trong backend (tuỳ chọn)

Consumer **tách process riêng** nhưng **cùng source** `backend/`:

| File | Vai trò |
|------|---------|
| `kafka/kafka.client.ts` | Init Kafka client, connect/disconnect |
| `kafka/kafka.constants.ts` | Cấu hình broker, topic, group |
| `consumers/main.ts` | Entry point consumer service |
| `consumers/kafka-consumer.service.ts` | Subscribe topic, log message |

```bash
npm run start:consumer:dev
# hoặc: npm run dev:consumer
```

---

## 10. Cách hoạt động của Produce / Consume

### Produce

```typescript
await producer.send({
  topic: 'demo-messages',
  messages: [{ value: 'Hello Kafka' }],
});
```

1. Producer kết nối broker
2. Gửi record vào topic
3. Broker ghi message vào partition và trả ack

### Consume

```typescript
await consumer.subscribe({ topic: 'demo-messages' });
await consumer.run({
  eachMessage: async ({ message }) => {
    console.log(message.value.toString());
  },
});
```

1. Consumer join consumer group
2. Kafka assign partition cho consumer
3. Consumer đọc message theo offset
4. Sau khi xử lý xong, commit offset (kafkajs tự commit mặc định)

---

## 11. Lưu ý khi phát triển

1. **Thứ tự khởi động**: Kafka → script demo (hoặc NestJS nếu dùng API)
2. **Topic**: chạy `kafka:single:setup` trước, hoặc topic tự tạo khi produce
3. **Single node**: phù hợp dev/local, không dùng production
4. **Consumer group**: nhiều consumer cùng group chia partition; khác group đọc độc lập
5. **Script vs NestJS**: demo học Kafka nên dùng script; NestJS dùng khi tích hợp app

---

## 12. Topic nội bộ `__consumer_offsets` (50 partitions)

Trên Kafka UI có thể thấy topic **`__consumer_offsets`** (tag **IN** = Internal) với **50 partitions**. **Không phải** topic do project tạo.

| | Giải thích |
|---|------------|
| **Mục đích** | Kafka lưu offset của consumer groups |
| **50 partitions** | Mặc định broker (`offsets.topic.num.partitions=50`) |
| **Dung lượng** | Rất nhỏ (metadata offset, không phải message app) |
| **Có xóa được?** | **Không** — consumer groups sẽ mất offset |

Topic cần quan tâm: `demo-messages` (demo này), `demo-partitions` (demo kafka2).

---

## 13. Troubleshooting

| Lỗi | Nguyên nhân | Cách xử lý |
|-----|-------------|------------|
| `Connection error` | Kafka chưa chạy | `npm run kafka:up` và đợi healthy |
| Không thấy message | Consumer chưa start hoặc đã commit offset cũ | Restart consumer hoặc dùng script consume với `fromBeginning: true` |
| Port 9092 bị chiếm | Service khác dùng port | Đổi port mapping trong `docker-compose.yml` hoặc tắt service conflict |
| Kafka UI không load | Port 8080 conflict hoặc UI start trước broker | Kiểm tra `docker compose ps`, xem `npm run kafka:ui:logs` |

---

## 14. Mở rộng tiếp theo

- Thêm nhiều topic cho từng domain (orders, notifications, ...)
- Dùng message key để đảm bảo ordering theo entity
- Thêm retry / dead-letter topic (DLQ) khi xử lý lỗi
- Tích hợp schema registry (Avro/JSON Schema)
- Scale lên Kafka cluster nhiều broker cho production

---

## 15. Tài liệu tham khảo

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [kafkajs - Node.js client](https://kafka.js.org/)
- [Apache Kafka Docker Image](https://hub.docker.com/r/apache/kafka)
- [UI for Apache Kafka (Provectus)](https://github.com/provectus/kafka-ui)
- [`scripts/README.md`](../scripts/README.md) — index demo scripts
