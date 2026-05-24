# Demo 1 — Single Node (Produce / Consume cơ bản)

Topic: `demo-messages` (1 partition)

**Không cần** chạy NestJS server hay consumer service.

## Workflow

```bash
# 0. Kafka phải đang chạy
npm run kafka:up

# 1. Tạo topic
npm run kafka:single:setup

# 2. Gửi message
npm run kafka:single:produce
npm run kafka:single:produce -- "Xin chao Kafka"

# 3. Nhận message
npm run kafka:single:consume
```

## Script trong folder này

| File | Lệnh npm | Mô tả |
|------|----------|-------|
| `setup.ts` | `kafka:single:setup` | Tạo topic `demo-messages` |
| `produce.ts` | `kafka:single:produce` | Gửi 1 message |
| `consume.ts` | `kafka:single:consume` | Lắng nghe và in message |

Tài liệu chi tiết: [`docs/kafka1.md`](../../docs/kafka1.md)
