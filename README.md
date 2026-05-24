# full-node

Monorepo: **NestJS backend**, **Kafka consumer service** (cùng source `backend/`), **React frontend**.

## Cấu trúc

```
full-node/
├── backend/
│   ├── src/
│   │   ├── main.ts
│   │   ├── kafka/
│   │   └── consumers/
│   └── scripts/
└── frontend/
```

## Chạy

```bash
cd backend && npm run kafka:up

# API
npm run dev:backend

# Consumer (terminal riêng, cùng source backend)
npm run dev:consumer

# Demo script (không cần NestJS)
cd backend && npm run kafka:single:produce
```
