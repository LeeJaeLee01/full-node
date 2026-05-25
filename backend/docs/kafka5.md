# Kafka Commit Offset — Auto-commit vs Manual Commit (NestJS)

Tài liệu mô tả **cơ chế commit offset**, cách **tắt auto-commit**, và triển khai **manual commit** trong NestJS sau khi xử lý nghiệp vụ thành công.

**Liên quan demo trước:**

| Demo | File | Nội dung |
|------|------|----------|
| kafka1 | `kafka1.md` | Produce/consume cơ bản |
| kafka2 | `kafka2.md` | Partition & offset |
| kafka4 | `kafka4.md` | Message key & ordering |
| **kafka5** | Tài liệu này | **Commit offset — manual trong NestJS** |

---

## 1. Offset commit là gì?

Khi consumer đọc message, Kafka lưu **vị trí đọc** (offset) cho từng `(consumer group, topic, partition)` trong topic nội bộ `__consumer_offsets`.

**Commit offset** = báo cho broker: *"Tôi đã xử lý xong tới đây, lần sau đọc tiếp từ offset sau."*

```
Partition log:  [0] [1] [2] [3] [4]
                      ↑
              committed offset = 3
              → lần sau đọc từ offset 3 (hoặc 4 tùy API)
```

Trong **kafkajs**, offset commit gửi lên broker là **offset của message tiếp theo** sẽ đọc:

```typescript
// Đã xử lý xong message tại offset 5
await consumer.commitOffsets([
  { topic, partition, offset: '6' }, // 5 + 1
]);
```

---

## 2. Auto-commit (mặc định)

`consumer.run()` mặc định `autoCommit: true`:

| Đặc điểm | Mô tả |
|----------|--------|
| Ai commit? | kafkajs tự commit theo chu kỳ |
| Khi nào? | Sau khi `eachMessage` **return** (không đợi logic nghiệp vụ phức tạp bên trong nếu fire-and-forget) |
| Rủi ro | Xử lý DB/API **thất bại sau khi handler return** → offset đã commit → **mất message** |
| Phù hợp | Demo, log đơn giản, không cần đảm bảo xử lý |

```typescript
await consumer.run({
  // autoCommit: true  ← mặc định
  eachMessage: async ({ message }) => {
    console.log(message.value?.toString());
    // kafkajs tự commit định kỳ
  },
});
```

---

## 3. Manual commit (khuyến nghị production)

Tắt auto-commit, **chỉ commit khi nghiệp vụ thành công**:

| Đặc điểm | Mô tả |
|----------|--------|
| `autoCommit` | `false` |
| Ai commit? | Code gọi `consumer.commitOffsets(...)` |
| Khi nào? | Sau DB transaction, sau gọi API thành công, v.v. |
| Lỗi nghiệp vụ | **Không commit** → message được **đọc lại** (at-least-once) |
| Semantics | **At-least-once delivery** (cần idempotent handler) |

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ Read msg    │ ──► │ Business logic   │ ──► │ commitOffsets() │
│ offset=N    │     │ (DB, API, ...)   │     │ offset=N+1      │
└─────────────┘     └──────────────────┘     └─────────────────┘
                            │
                     fail ──┴──► KHÔNG commit → redelivery
```

---

## 4. Triển khai trong project (NestJS)

### 4.1 Cấu trúc file

| File | Vai trò |
|------|---------|
| `kafka/kafka.client.ts` | Kafka consumer client |
| `kafka/kafka.constants.ts` | `KAFKA_COMMIT_TOPIC`, `KAFKA_GROUP_ID` |
| `consumers/kafka-consumer.service.ts` | `autoCommit: false` + `commitOffsets` |
| `consumers/message-processor.service.ts` | Logic nghiệp vụ mô phỏng |
| `consumers/main.ts` | Entry process consumer |

### 4.2 Tắt auto-commit + manual commit

```typescript
await consumer.run({
  autoCommit: false,
  eachMessage: async ({ topic, partition, message }) => {
    const value = message.value?.toString() ?? '';

    await this.messageProcessor.process(value); // nghiệp vụ

    const nextOffset = (BigInt(message.offset) + 1n).toString();
    await consumer.commitOffsets([
      { topic, partition, offset: nextOffset },
    ]);
  },
});
```

Code thực tế: `src/consumers/kafka-consumer.service.ts`.

### 4.3 Xử lý lỗi nghiệp vụ

`MessageProcessorService` ném `BusinessMessageError` khi message là `FAIL` hoặc `{ "fail": true }`:

- Consumer **log cảnh báo**, **không** gọi `commitOffsets`
- Message **không bị đánh dấu đã xử lý** → consumer đọc lại (cùng partition/offset)

Lỗi không mong đợi (bug) vẫn `throw` để kafkajs có thể retry theo cấu hình.

### 4.4 Biến môi trường

| Biến | Mặc định | Ý nghĩa |
|------|----------|---------|
| `KAFKA_COMMIT_TOPIC` | `demo-manual-commit` | Topic demo manual commit |
| `KAFKA_COMMIT_PARTITION_COUNT` | `1` | Partition (script setup) |
| `KAFKA_GROUP_ID` | `full-node-consumer-group` | Consumer group |
| `KAFKA_BROKERS` | `localhost:9092` | Broker |

Consumer subscribe `KAFKA_COMMIT_TOPIC` (fallback `KAFKA_TOPIC` nếu không set).

Thêm vào `.env`:

```env
KAFKA_COMMIT_TOPIC=demo-manual-commit
KAFKA_GROUP_ID=full-node-manual-commit-group
```

> Dùng **consumer group riêng** khi thử nghiệm để offset không bị ảnh hưởng bởi lần chạy cũ.

---

## 5. Workflow kiểm chứng

### Bước 1: Kafka + topic

```bash
cd backend
npm run kafka:up
npm run kafka:manual-commit:setup
```

### Bước 2: Produce test messages

```bash
npm run kafka:manual-commit:produce
```

Gửi 4 message:

| # | Nội dung | Kỳ vọng |
|---|----------|---------|
| 1 | `{ action: "create-order", ... }` | Commit OK |
| 2 | `{ action: "pay-order", ... }` | Commit OK |
| 3 | `FAIL` | **Không commit** |
| 4 | `{ action: "ship-order", ... }` | Chỉ commit sau khi #3 được xử lý lại thành công hoặc skip |

### Bước 3: Chạy NestJS consumer

```bash
npm run start:consumer:dev
# hoặc từ root: npm run dev:consumer
```

Log mẫu:

```
[KafkaConsumerService] Received [...] offset 0: {"action":"create-order"...}
[KafkaConsumerService] Manual commit OK → next offset 1
[KafkaConsumerService] Received [...] offset 1: {"action":"pay-order"...}
[KafkaConsumerService] Manual commit OK → next offset 2
[KafkaConsumerService] Received [...] offset 2: FAIL
[MessageProcessorService] Business failed — offset NOT committed
```

Message `FAIL` tại offset 2 **không commit** → mỗi lần consumer chạy lại vẫn thấy offset 2 (hoặc consumer loop đọc lại tùy cấu hình pause).

### Bước 4: Kafka UI

1. **Consumer Groups** → group của bạn → Lag / Current offset
2. Sau 2 message thành công: committed offset = 2
3. Sau `FAIL`: offset **dừng** tại 2 (chưa commit message lỗi)

---

## 6. So sánh Auto vs Manual

| | Auto-commit | Manual commit |
|---|-------------|---------------|
| Cấu hình | Mặc định | `autoCommit: false` |
| Thời điểm commit | Theo interval kafkajs | Sau logic nghiệp vụ |
| Mất message khi lỗi DB | Có thể | Giảm (redelivery) |
| Duplicate khi retry | Ít hơn | Có thể (cần idempotent) |
| Độ phức tạp | Thấp | Cao hơn |
| Production | Log đơn giản | **Khuyến nghị** cho nghiệp vụ quan trọng |

---

## 7. At-least-once và idempotency

Manual commit → **at-least-once**:

- Crash **sau** xử lý DB **trước** commit → message đọc lại → có thể **duplicate**
- Giải pháp: idempotent key (`orderId`), unique constraint DB, hoặc outbox pattern

---

## 8. Xử lý lâu (heartbeat)

Nếu logic nghiệp vụ > `session.timeout.ms`, truyền `heartbeat` trong `eachMessage`:

```typescript
eachMessage: async ({ heartbeat, ...rest }) => {
  await longRunningTask();
  await heartbeat();
  await consumer.commitOffsets([...]);
};
```

Hoặc tăng `sessionTimeout` khi tạo consumer:

```typescript
this.kafka.consumer({
  groupId: KAFKA_GROUP_ID,
  sessionTimeout: 30000,
});
```

---

## 9. Script CLI

```
backend/scripts/manual-commit/
├── setup.ts
├── produce.ts
└── README.md
```

| Lệnh | Mô tả |
|------|-------|
| `npm run kafka:manual-commit:setup` | Tạo topic `demo-manual-commit` |
| `npm run kafka:manual-commit:produce` | Gửi message test (có `FAIL`) |
| `npm run start:consumer:dev` | NestJS consumer manual commit |

---

## 10. Troubleshooting

| Vấn đề | Nguyên nhân | Cách xử lý |
|--------|-------------|------------|
| Message đọc lại mãi | Không commit message lỗi | Sửa/xử lý DLQ, hoặc skip có chủ đích |
| Offset nhảy qua message lỗi | Vẫn bật auto-commit | Kiểm tra `autoCommit: false` |
| Duplicate xử lý | Redelivery sau crash | Idempotent handler |
| `UNKNOWN_TOPIC_OR_PARTITION` | Topic chưa tạo | `kafka:manual-commit:setup` |
| Consumer không đọc từ đầu | `fromBeginning: false` | Reset group offset trên UI hoặc đổi `KAFKA_GROUP_ID` |

Reset offset (dev): Kafka UI → Consumer Groups → Reset offsets.

---

## 11. Liên quan

- [`kafka1.md`](./kafka1.md) — consume cơ bản, NestJS consumer tùy chọn
- [`kafka2.md`](./kafka2.md) — partition & offset
- [`kafka4.md`](./kafka4.md) — message key & ordering
- [`kafka5.md`](./kafka5.md) — manual commit (tài liệu này)
- [`scripts/manual-commit/README.md`](../scripts/manual-commit/README.md)

---

## 12. Tài liệu tham khảo

- [Kafka Consumer Offsets](https://kafka.apache.org/documentation/#consumerconfigs)
- [kafkajs — Consumer](https://kafka.js.org/docs/consuming)
- [Delivery semantics](https://kafka.apache.org/documentation/#semantics)
