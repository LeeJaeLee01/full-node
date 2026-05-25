# Demo 4 — Message Keys & Ordering

Topic: `demo-key-order` (3 partitions)

Gửi chuỗi sự kiện **có key** (ví dụ `user-1`, `user-2`) và kiểm chứng:
- Cùng key → cùng partition
- Cùng key → offset tăng dần (đúng thứ tự thời gian)

## Workflow

```bash
npm run kafka:up
npm run kafka:key-order:setup
npm run kafka:key-order:produce
npm run kafka:key-order:consume
```

Tài liệu: [`docs/kafka4.md`](../../docs/kafka4.md)
