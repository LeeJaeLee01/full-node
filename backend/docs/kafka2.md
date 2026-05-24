# Kafka Partition & Offset — Round-robin Producer (không key)

Tài liệu này mô tả cách vận hành **Partition** và **Offset** trong project, với bài thực hành:

- Tạo topic **`demo-partitions`** với **3 partitions**
- Producer gửi message **không có key**
- Phân tích cách message được phân bổ **round-robin** qua các partition

---

## 1. Partition và Offset là gì?

### Partition

- Mỗi **topic** được chia thành nhiều **partition** (phân vùng)
- Partition cho phép Kafka xử lý song song và scale throughput
- Message trong cùng partition được sắp xếp theo thứ tự (ordering theo partition)

```
Topic: demo-partitions
├── Partition 0  →  [msg-1] [msg-4] [msg-7]
├── Partition 1  →  [msg-2] [msg-5] [msg-8]
└── Partition 2  →  [msg-3] [msg-6] [msg-9]
```

### Offset

- **Offset** là vị trí (index) của message **trong một partition**
- Offset bắt đầu từ `0` và tăng dần
- Consumer dùng offset để biết đã đọc tới đâu và commit tiến độ

| Khái niệm | Ví dụ |
|-----------|-------|
| Topic | `demo-partitions` |
| Partition | `0`, `1`, `2` |
| Offset | `0`, `1`, `2`, ... (riêng từng partition) |

---

## 2. Producer chọn Partition như thế nào?

Khi gửi message, Kafka producer quyết định message vào partition nào:

| Trường hợp | Cách chọn partition |
|------------|---------------------|
| **Có key** | `hash(key) % số_partition` → cùng key luôn vào cùng partition |
| **Không có key** | Round-robin / sticky partitioner (tùy client) |

Trong project này, demo **không key** dùng **`LegacyPartitioner`** của kafkajs → **round-robin**:

```
Message 1 → Partition 0
Message 2 → Partition 1
Message 3 → Partition 2
Message 4 → Partition 0
Message 5 → Partition 1
...
```

> **Lưu ý:** kafkajs v2 mặc định dùng sticky partitioner (không phải round-robin). Project cố ý dùng `Partitioners.LegacyPartitioner` để quan sát round-robin rõ ràng.

---

## 3. Cấu hình trong project

| Biến môi trường | Mặc định | Ý nghĩa |
|-----------------|----------|---------|
| `KAFKA_PARTITION_TOPIC` | `demo-partitions` | Topic demo 3 partitions |
| `KAFKA_PARTITION_COUNT` | `3` | Số partition |
| `KAFKA_PARTITION_GROUP_ID` | `full-node-partition-consumer-group` | Consumer group cho demo partition |

---

## 4. Cấu trúc code

```
backend/scripts/partition/    # Demo CLI (tài liệu này)
└── setup.ts, produce.ts, consume.ts
```

> REST API `/partitions/*` đã bỏ khỏi `backend/`. Demo partition chỉ qua script.

---

## 5. Tạo Topic 3 Partitions

### Script (khuyên dùng)

```bash
cd backend
npm run kafka:partition:setup
```

Output mẫu:

```
[setup] Created topic "demo-partitions" with 3 partitions
[setup] partition=0 leader=1
[setup] partition=1 leader=1
[setup] partition=2 leader=1
```

### Cách 2: Kafka UI

Mở **http://localhost:8080** → **Topics** → tìm `demo-partitions` → xem **Partitions = 3**

---

## 6. Producer không key — Round-robin

### Script (khuyên dùng để quan sát)

Gửi 9 message (mặc định):

```bash
npm run kafka:partition:produce
```

Gửi số lượng tùy chọn:

```bash
npm run kafka:partition:produce -- 12
```

Output mẫu:

```
[produce] #1 value="message-1" → partition=0, offset=0
[produce] #2 value="message-2" → partition=1, offset=0
[produce] #3 value="message-3" → partition=2, offset=0
[produce] #4 value="message-4" → partition=0, offset=1
[produce] #5 value="message-5" → partition=1, offset=1
[produce] #6 value="message-6" → partition=2, offset=1
[produce] #7 value="message-7" → partition=0, offset=2
[produce] #8 value="message-8" → partition=1, offset=2
[produce] #9 value="message-9" → partition=2, offset=2

[produce] Distribution summary:
  partition 0: 3 message(s), offsets=[0, 1, 2]
  partition 1: 3 message(s), offsets=[0, 1, 2]
  partition 2: 3 message(s), offsets=[0, 1, 2]

[produce] Result: messages are evenly distributed (round-robin)
```

### Script produce

## 7. Phân tích Round-robin

### Quy luật quan sát được

Với topic **3 partitions**, gửi **không key**, dùng **LegacyPartitioner**:

| Message | Partition | Offset (trong partition đó) |
|---------|-----------|-----------------------------|
| message-1 | 0 | 0 |
| message-2 | 1 | 0 |
| message-3 | 2 | 0 |
| message-4 | 0 | 1 |
| message-5 | 1 | 1 |
| message-6 | 2 | 1 |
| message-7 | 0 | 2 |
| message-8 | 1 | 2 |
| message-9 | 2 | 2 |

**Kết luận:**

1. **Partition rotation:** `0 → 1 → 2 → 0 → 1 → 2 → ...`
2. **Offset** tăng **độc lập** trên từng partition (mỗi partition có dãy offset riêng)
3. Với `N` message và `P` partition, mỗi partition nhận khoảng `N/P` message (chênh lệch tối đa 1)

### Công thức đơn giản

```
partition_index = (message_index - 1) % số_partition
```

Ví dụ message thứ 5: `(5 - 1) % 3 = 1` → partition 1

---

## 8. Consume và quan sát Offset

```bash
npm run kafka:partition:consume
# hoặc đọc 12 message rồi dừng
npm run kafka:partition:consume -- 12
```

Output mẫu:

```
[consume] partition=0 offset=0 value="message-1"
[consume] partition=1 offset=0 value="message-2"
[consume] partition=2 offset=0 value="message-3"
...
[consume] Partition / offset summary:
  partition 0: 3 message(s), offsets=[0, 1, 2]
  partition 1: 3 message(s), offsets=[0, 1, 2]
  partition 2: 3 message(s), offsets=[0, 1, 2]
```

---

## 9. Workflow end-to-end (Script — không cần NestJS)

```bash
# 1. Kafka
npm run kafka:up

# 2. Tạo topic 3 partitions
npm run kafka:partition:setup

# 3. Produce 9 message không key
npm run kafka:partition:produce

# 4. Consume và xem partition/offset
npm run kafka:partition:consume

# 5. (Tuỳ chọn) Kafka UI: http://localhost:8080 → demo-partitions
```

Chi tiết script: [`scripts/partition/README.md`](../scripts/partition/README.md)

---

## 10. Script tổng hợp

| Lệnh | Mô tả |
|------|-------|
| `kafka:partition:setup` | Tạo topic 3 partitions |
| `kafka:partition:produce` | Produce không key + phân tích phân bổ |
| `kafka:partition:consume` | Consume + hiển thị partition/offset |

---

## 11. So sánh: có key vs không key

| | Không key (demo này) | Có key |
|---|---------------------|--------|
| Partitioner | LegacyPartitioner (round-robin) | Hash key |
| Phân bổ | Đều qua các partition | Cùng key → cùng partition |
| Ordering | Không đảm bảo global order | Order theo key trong 1 partition |
| Use case | Load balancing đơn giản | Event của cùng entity (userId, orderId) |

---

## 12. Troubleshooting

| Vấn đề | Nguyên nhân | Cách xử lý |
|--------|-------------|------------|
| Topic có 1 partition thay vì 3 | Topic đã tạo trước với 1 partition | Xóa topic trên Kafka UI hoặc CLI, chạy lại setup |
| Không round-robin đều | Dùng default partitioner thay vì Legacy | Project đã cấu hình `Partitioners.LegacyPartitioner` |
| `UNKNOWN_TOPIC_OR_PARTITION` | Topic chưa tạo | Chạy `npm run kafka:partition:setup` |
| Offset không bắt đầu từ 0 | Topic đã có message cũ | Xóa topic và tạo lại, hoặc ghi nhận offset hiện tại |

Xóa topic bằng CLI:

```bash
docker exec -it full-node-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete --topic demo-partitions
```

---

## 13. Liên quan

- [`kafka1.md`](./kafka1.md) — setup Kafka single node, produce/consume cơ bản
- [`kafka2.md`](./kafka2.md) — partition, offset, round-robin (tài liệu này)
- [`scripts/README.md`](../scripts/README.md) — index demo scripts

---

## 14. Tài liệu tham khảo

- [Kafka Partition Docs](https://kafka.apache.org/documentation/#intro_concepts_and_terms)
- [kafkajs Partitioners](https://kafka.js.org/docs/producing#optionally-specify-a-partitioner)
- [kafkajs Migration v2 — default partitioner changed](https://kafka.js.org/docs/migration-guide-v2.0.0#producer-new-default-partitioner)
