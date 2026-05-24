# Demo 2 — Partition & Offset (Round-robin)

Topic: `demo-partitions` (3 partitions)

**Không cần** chạy NestJS server hay consumer service.

## Workflow

```bash
# 0. Kafka phải đang chạy
npm run kafka:up

# 1. Tạo topic 3 partitions
npm run kafka:partition:setup

# 2. Produce không key + phân tích phân bổ
npm run kafka:partition:produce
npm run kafka:partition:produce -- 12

# 3. Consume + xem partition/offset
npm run kafka:partition:consume
npm run kafka:partition:consume -- 12
```

## Script trong folder này

| File | Lệnh npm | Mô tả |
|------|----------|-------|
| `setup.ts` | `kafka:partition:setup` | Tạo topic `demo-partitions` (3 partitions) |
| `produce.ts` | `kafka:partition:produce` | Gửi N message không key, in phân bổ round-robin |
| `consume.ts` | `kafka:partition:consume` | Đọc message, hiển thị partition + offset |

Tài liệu chi tiết: [`docs/kafka2.md`](../../docs/kafka2.md)
