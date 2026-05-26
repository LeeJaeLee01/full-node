# Kafka Demo Scripts

Các demo Kafka chạy **độc lập bằng script CLI** — không cần NestJS server.

## Cấu trúc

```
scripts/
├── _shared/           # Helper dùng chung
├── single-node/       # Demo 1: produce/consume cơ bản
├── partition/         # Demo 2: partition, offset, round-robin
└── consumer-group/    # Demo 3: consumer group, load sharing
```

## Docker (chung cho mọi demo)

```bash
npm run kafka:up          # Start Kafka + Kafka UI
npm run kafka:down        # Stop
npm run kafka:logs        # Log broker
npm run kafka:ui:logs     # Log Kafka UI
```

Kafka UI: **http://localhost:8080**

---

## Demo 1 — Single Node

Topic: `demo-messages` (1 partition)

```bash
npm run kafka:single:setup
npm run kafka:single:produce
npm run kafka:single:produce -- "Hello"
npm run kafka:single:consume
```

→ Chi tiết: [`single-node/README.md`](./single-node/README.md) | [`docs/kafka1.md`](../docs/kafka1.md)

---

## Demo 2 — Partition & Offset

Topic: `demo-partitions` (3 partitions)

```bash
npm run kafka:partition:setup
npm run kafka:partition:produce
npm run kafka:partition:consume
```

→ Chi tiết: [`partition/README.md`](./partition/README.md) | [`docs/kafka2.md`](../docs/kafka2.md)

---

## Demo 3 — Consumer Group & Chia sẻ tải

Topic: `demo-consumer-group` (3 partitions), 4 consumer cùng group

```bash
npm run kafka:group:setup
npm run kafka:group:produce
npm run kafka:group:consume -- 1   # mở thêm terminal 2,3,4 với id 2–4
# hoặc một lệnh:
npm run kafka:group:consume-all
npm run kafka:group:describe
```

→ Chi tiết: [`consumer-group/README.md`](./consumer-group/README.md) | [`docs/kafka3.md`](../docs/kafka3.md)

---

## NestJS (tuỳ chọn)

| Demo | NestJS | Script |
|------|--------|--------|
| Single node produce | — | `kafka:single:produce` |
| Single node consume | `backend/` → `start:consumer:dev` | `kafka:single:consume` |
| Partition | — | `kafka:partition:*` |
| Consumer group | — | `kafka:group:*` |
