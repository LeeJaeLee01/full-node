# Demo 5 — Manual Commit Offset

Topic: `demo-manual-commit` (1 partition)

Consumer **NestJS** tắt auto-commit, commit thủ công sau khi xử lý nghiệp vụ thành công.

## Workflow

```bash
npm run kafka:up
npm run kafka:manual-commit:setup
npm run kafka:manual-commit:produce

# Terminal khác — NestJS consumer (manual commit)
npm run start:consumer:dev
```

Message `FAIL` sẽ không được commit → consumer đọc lại khi restart.

Tài liệu: [`docs/kafka5.md`](../../docs/kafka5.md)
