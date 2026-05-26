# Demo 3 — Consumer Group & Chia sẻ tải

Topic: `demo-consumer-group` (3 partitions)  
Consumer group: `full-node-consumer-group-demo`

**Không cần** chạy NestJS server.

## Quan sát chính

Với **3 partitions** và **4 consumer** trong **cùng một group**:

- Tối đa **3 consumer** được assign partition và xử lý message
- **Consumer thứ 4** thường **IDLE** (không có partition) vì Kafka chỉ gán mỗi partition cho **một** consumer trong group

## Workflow nhanh (một terminal)

```bash
npm run kafka:up
npm run kafka:group:setup
npm run kafka:group:produce

# 4 consumer trong 1 process — xem assignment ngay khi GROUP_JOIN
npm run kafka:group:consume-all
```

## Workflow đầy đủ (4 terminal — giống production)

**Terminal 1–4** (chạy lần lượt hoặc gần đồng thời):

```bash
npm run kafka:group:consume -- 1
npm run kafka:group:consume -- 2
npm run kafka:group:consume -- 3
npm run kafka:group:consume -- 4
```

**Terminal 5** — produce message:

```bash
npm run kafka:group:produce
npm run kafka:group:produce -- 15
```

**Terminal 6** (tuỳ chọn) — xem trạng thái group:

```bash
npm run kafka:group:describe
```

## Script trong folder này

| File | Lệnh npm | Mô tả |
|------|----------|-------|
| `setup.ts` | `kafka:group:setup` | Tạo topic 3 partitions |
| `produce.ts` | `kafka:group:produce` | Gửi N message (round-robin) |
| `consume.ts` | `kafka:group:consume -- <id>` | Một consumer (id 1–16) |
| `consume-all.ts` | `kafka:group:consume-all` | 4 consumer trong một process |
| `describe.ts` | `kafka:group:describe` | Mô tả assignment của group |

Tài liệu chi tiết: [`docs/kafka3.md`](../../docs/kafka3.md)
