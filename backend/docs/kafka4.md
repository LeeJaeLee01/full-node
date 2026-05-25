# Kafka Message Keys & Ordering — Khái niệm, Triển khai và Sử dụng

Tài liệu này mô tả cách dùng **Message Key** để đảm bảo **thứ tự sự kiện** theo entity (ví dụ `user_id`), và cách kiểm chứng trong project.

**So sánh demo trước:**

| Demo | File | Key | Ordering |
|------|------|-----|----------|
| kafka2 | `kafka2.md` | Không có | Round-robin, không đảm bảo global order |
| **kafka4** | Tài liệu này | **Có** (`user-1`, `user-2`...) | **Cùng key → cùng partition → đúng thứ tự** |

---

## 1. Message Key là gì?

**Message Key** là một chuỗi (bytes) gắn kèm mỗi record khi produce. Kafka dùng key để quyết định message vào **partition nào**.

```typescript
await producer.send({
  topic: 'demo-key-order',
  messages: [
    { key: 'user-1', value: 'đặt hàng' },
    { key: 'user-2', value: 'đăng ký' },
    { key: 'user-1', value: 'thanh toán' },  // cùng user-1 → cùng partition
  ],
});
```

### Khi nào cần key?

| Use case | Key gợi ý |
|----------|-----------|
| Sự kiện theo user | `user_id` |
| Đơn hàng | `order_id` |
| Thiết bị IoT | `device_id` |
| Log theo session | `session_id` |

**Không cần key** khi chỉ cần load balancing đều (xem `kafka2.md`).

---

## 2. Key ảnh hưởng Partition như thế nào?

Kafka (và kafkajs mặc định) dùng:

```
partition = hash(key) % số_partition
```

Với topic **3 partitions**:

```
hash('user-1') % 3 → luôn cùng 1 số (ví dụ partition 2)
hash('user-2') % 3 → luôn cùng 1 số khác (ví dụ partition 0)
hash('user-3') % 3 → luôn cùng 1 số khác (ví dụ partition 1)
```

**Hệ quả:**

- Mọi message của `user-1` vào **cùng một partition**
- Thứ tự ghi vào partition = thứ tự produce (trong cùng producer, không lỗi retry phức tạp)
- Consumer đọc partition đó sẽ thấy event `user-1` **đúng thứ tự thời gian**

```
Partition 0:  user-2 event-1 → user-2 event-2 → user-2 event-3
Partition 1:  user-3 event-1 → user-3 event-2 → user-3 event-3
Partition 2:  user-1 event-1 → user-1 event-2 → user-1 event-3 → user-1 event-4
```

> **Lưu ý:** Thứ tự chỉ đảm bảo **trong một partition**. Không có global order giữa `user-1` và `user-2`.

---

## 3. Ordering (thứ tự) trong Kafka

| Phạm vi | Có ordering? |
|---------|--------------|
| Trong **một partition** | Có (theo offset tăng dần) |
| Trong **một key** (cùng partition) | Có |
| Toàn **topic** (nhiều key/partition) | Không đảm bảo |

Để có **chuỗi sự kiện đúng thứ tự** cho một user:

1. Dùng **key = user_id**
2. Gửi message **tuần tự** (hoặc một producer cho key đó)
3. Consumer đọc partition tương ứng → offset tăng dần

---

## 4. Cấu hình trong project

| Biến môi trường | Mặc định | Ý nghĩa |
|-----------------|----------|---------|
| `KAFKA_KEY_TOPIC` | `demo-key-order` | Topic demo key & ordering |
| `KAFKA_KEY_PARTITION_COUNT` | `3` | Số partition |
| `KAFKA_KEY_GROUP_ID` | `full-node-key-order-consumer-group` | Consumer group |

Thêm vào `.env` (copy từ `.env.example`).

---

## 5. Cấu trúc script demo

```
backend/scripts/key-order/
├── setup.ts      # Tạo topic 3 partitions
├── produce.ts    # Gửi chuỗi event có key + kiểm chứng
├── consume.ts    # Đọc lại và xác nhận thứ tự
└── README.md
```

Demo dùng **default partitioner** (hash key) — **không** dùng `LegacyPartitioner` như kafka2.

---

## 6. Triển khai — Workflow end-to-end

### Bước 1: Khởi động Kafka

```bash
cd backend
npm run kafka:up
```

Kafka UI: **http://localhost:8080**

### Bước 2: Tạo topic

```bash
npm run kafka:key-order:setup
```

Topic `demo-key-order` với **3 partitions**.

### Bước 3: Produce chuỗi sự kiện có key

```bash
npm run kafka:key-order:produce
```

Script gửi **12 event** xen kẽ 3 user (`user-1`, `user-2`, `user-3`), mỗi event có:
- `key`: user id
- `value`: mô tả hành động (đặt hàng, thanh toán, ...)
- `seq`: số thứ tự gửi (để đối chiếu)

Output mẫu:

```
[key-order/produce] #1 key="user-1" → partition=2, offset=0 | user-1 đặt hàng #1
[key-order/produce] #2 key="user-2" → partition=0, offset=0 | user-2 đăng ký
[key-order/produce] #3 key="user-1" → partition=2, offset=1 | user-1 thanh toán
...
[key-order/produce] Same key → same partition: PASS
[key-order/produce] Same key → offset tăng dần (đúng thứ tự): PASS
```

### Bước 4: Consume và kiểm chứng

```bash
npm run kafka:key-order:consume
```

Đọc từ đầu topic, nhóm theo key, kiểm tra:
- `stable=YES` — cùng key cùng partition
- `ordered=YES` — offset tăng dần khi đọc theo key

### Bước 5: Xem trên Kafka UI

1. **Topics** → `demo-key-order` → **Messages**
2. Filter / browse theo partition
3. Thấy message cùng key nằm cùng partition
4. **Consumer Groups** → offset theo partition

---

## 7. Kịch bản kiểm chứng chi tiết

Script `produce.ts` gửi theo thứ tự:

| Seq | Key | Sự kiện |
|-----|-----|----------|
| 1 | user-1 | đặt hàng #1 |
| 2 | user-2 | đăng ký |
| 3 | user-1 | thanh toán |
| 4 | user-3 | đăng ký |
| 5 | user-2 | đặt hàng #1 |
| 6 | user-1 | giao hàng |
| ... | ... | ... |

**Kết quả mong đợi:**

1. **Cùng partition:** Mọi dòng `user-1` có cùng `partition=X`
2. **Đúng thứ tự:** Offset của `user-1` tăng dần: `0 → 1 → 2 → 3` (khớp seq 1 → 3 → 6 → 9)
3. **Xen kẽ key khác:** `user-2`, `user-3` có thể vào partition khác, không ảnh hưởng thứ tự `user-1`

---

## 8. So sánh: Có key vs Không key

| | Không key (kafka2) | Có key (kafka4) |
|---|-------------------|-----------------|
| Chọn partition | Round-robin | Hash(key) |
| Cùng entity cùng partition | Không đảm bảo | **Đảm bảo** |
| Ordering theo entity | Không | **Có** (trong partition) |
| Use case | Log chung, metric | Event sourcing, order flow, user activity |

---

## 9. Lưu ý khi production

1. **Chọn key ổn định** — `user_id` tốt hơn email có thể đổi
2. **Key skew** — Một key quá hot → một partition quá tải (cân nhắc partition count)
3. **Null key** — Message không key dùng round-robin, không có ordering
4. **Nhiều producer** — Cùng key từ nhiều producer vẫn cùng partition; thứ tự vẫn OK per partition nhưng interleaving phức tạp hơn
5. **Retry / idempotent** — Cấu hình producer idempotence nếu cần exactly-once

---

## 10. Troubleshooting

| Vấn đề | Nguyên nhân | Cách xử lý |
|--------|-------------|------------|
| Cùng key vào partition khác | Key không stable (đổi format) | Chuẩn hóa key trước khi send |
| Offset không tăng dần | Retry duplicate, hoặc nhiều producer | Kiểm tra `enable.idempotence`, gửi tuần tự |
| `UNKNOWN_TOPIC_OR_PARTITION` | Topic chưa tạo | `npm run kafka:key-order:setup` |
| Consume lộn xộn | Đọc nhiều partition song song | Bình thường — ordering chỉ trong từng partition/key |

---

## 11. Script tổng hợp

| Lệnh | Mô tả |
|------|-------|
| `npm run kafka:key-order:setup` | Tạo topic `demo-key-order` (3 partitions) |
| `npm run kafka:key-order:produce` | Gửi event có key + in kết quả kiểm chứng |
| `npm run kafka:key-order:consume` | Consume và kiểm tra thứ tự theo key |

---

## 12. Liên quan

- [`kafka1.md`](./kafka1.md) — Kafka single node, produce/consume cơ bản
- [`kafka2.md`](./kafka2.md) — Partition, offset, round-robin **không key**
- [`kafka4.md`](./kafka4.md) — Message key & ordering (tài liệu này)
- [`scripts/key-order/README.md`](../scripts/key-order/README.md) — Hướng dẫn chạy nhanh

---

## 13. Tài liệu tham khảo

- [Kafka Message Keys](https://kafka.apache.org/documentation/#intro_concepts_and_terms)
- [kafkajs — Producing with keys](https://kafka.js.org/docs/producing#message-key)
- [Ordering guarantees](https://kafka.apache.org/documentation/#semantics)
