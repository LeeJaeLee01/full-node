# Amazon SQS — Khái niệm và lưu ý thực hành

## 1. SQS là gì?

**Amazon SQS** (Simple Queue Service) là dịch vụ **hàng đợi tin nhắn** (message queue) do AWS quản lý.

**Mục tiêu:**

- Tách **producer** và **consumer** (giảm coupling)
- Làm **đệm** hệ thống khi có đỉnh tải (traffic spike)
- Tăng độ bền và khả năng scale cho luồng xử lý **bất đồng bộ** (asynchronous workflows)

SQS **không** đảm bảo *exactly-once* theo nghĩa tuyệt đối với **Standard queue**. Bạn cần thiết kế **idempotency** ở phía consumer.

---

## 2. Hai loại queue chính

### Standard Queue

- Throughput rất cao
- **At-least-once delivery** — có thể nhận trùng message
- **Best-effort ordering** — không đảm bảo đúng thứ tự tuyệt đối
- Phù hợp phần lớn use case cần scale

### FIFO Queue

- Đảm bảo thứ tự message trong cùng `MessageGroupId`
- **Exactly-once processing** theo cơ chế dedup của FIFO (trong dedup window)
- Throughput thấp hơn Standard (đã được cải thiện với high-throughput FIFO)
- Bắt buộc dùng `MessageGroupId`, thường kèm `MessageDeduplicationId`

| | Standard | FIFO |
|---|----------|------|
| Thứ tự | Không đảm bảo | Có (theo group) |
| Trùng lặp | Có thể | Giảm (dedup) |
| Throughput | Cao | Thấp hơn |
| Đuôi tên queue | Tùy ý | Phải kết thúc `.fifo` |

---

## 3. Các khái niệm cốt lõi

### Vòng đời message (Message lifecycle)

1. Producer gọi `SendMessage`
2. Message được lưu trong queue
3. Consumer gọi `ReceiveMessage`
4. Message vào trạng thái **invisible** trong khoảng **Visibility Timeout**
5. Consumer xử lý xong thì gọi `DeleteMessage`

Nếu **không** `DeleteMessage` trước khi hết Visibility Timeout, message sẽ **hiện lại** và có thể được xử lý lần nữa.

```
Producer ──SendMessage──► Queue ──ReceiveMessage──► Consumer
                              │                        │
                              │    (invisible)         │
                              │◄── DeleteMessage ──────┘
                              │     (sau khi xử lý OK)
```

### Visibility Timeout

- Khoảng thời gian tạm **“khóa”** message sau khi đã receive
- Đặt timeout phù hợp với thời gian xử lý trung bình + buffer
- Job dài có thể dùng `ChangeMessageVisibility` để **gia hạn** timeout

### Long Polling

- Đặt `ReceiveMessageWaitTimeSeconds` (tối đa **20 giây**) để giảm response rỗng
- Giảm chi phí và latency so với short polling
- **Khuyến nghị** bật long polling

### Message Retention Period

- Thời gian message được giữ trong queue (**1 phút → 14 ngày**)
- Cân bằng giữa nhu cầu retry/recovery và chi phí lưu trữ

### Delay Seconds

- Trì hoãn việc message có thể được consume
- Dùng cho retry có kiểm soát hoặc workflow cần delay

### In-flight messages

- Message đã **receive** nhưng chưa **delete**
- Quá cao có thể bị throttle / hạn chế nhận thêm message
- Cần monitor để phát hiện consumer chậm hoặc treo

---

## 4. Dead-Letter Queue (DLQ)

**DLQ** dùng để gom message xử lý thất bại nhiều lần.

**Cấu hình quan trọng:**

- Redrive policy với `maxReceiveCount`
- Queue chính → DLQ **cùng loại** (Standard với Standard, FIFO với FIFO)

**Lợi ích:**

- Cô lập **poison messages**
- Không chặn cả pipeline
- Hỗ trợ troubleshooting và replay

**Lưu ý:** Không “bỏ mặc” DLQ — cần quy trình đọc, phân tích và re-drive.

---

## 5. Những điều cần biết khi dùng SQS trong production

### 5.1 Idempotency là bắt buộc

Do có khả năng **giao trùng** (đặc biệt Standard), consumer phải **idempotent**:

- Dùng business key / idempotency key
- Lưu trạng thái đã xử lý (DB / cache)
- Tránh side effect lặp (charge hai lần, gửi email hai lần, …)

### 5.2 Tính đúng Visibility Timeout

- Timeout **quá ngắn** → message quay lại sớm, xử lý trùng
- Timeout **quá dài** → failover chậm, tăng độ trễ
- Mục tiêu: đủ để xử lý + retry nhanh khi consumer chết

### 5.3 Chọn Standard vs FIFO đúng bài toán

- Ưu tiên **Standard** nếu cần throughput cao và chấp nhận out-of-order / duplicate
- Chọn **FIFO** khi nghiệp vụ bắt buộc ordering hoặc dedup chặt
- FIFO: thiết kế `MessageGroupId` hợp lý để tránh **hot group**

### 5.4 Batch để tối ưu chi phí / throughput

- Dùng `SendMessageBatch`, `DeleteMessageBatch` khi phù hợp
- Consumer bật batch processing để giảm số lần gọi API
- Vẫn phải xử lý từng message fail trong batch

### 5.5 Monitoring và alerting

**Metric quan trọng** (CloudWatch):

| Metric | Ý nghĩa |
|--------|---------|
| `ApproximateNumberOfMessagesVisible` | Backlog (message chờ xử lý) |
| `ApproximateAgeOfOldestMessage` | Độ trễ message cũ nhất |
| `NumberOfMessagesReceived` / `Deleted` | Throughput consume |
| Số message vào DLQ | Lỗi xử lý tích lũy |

Đặt alert khi backlog tăng bất thường, oldest age vượt SLA, DLQ tăng đột biến.

### 5.6 Bảo mật

- Bật mã hóa (SSE-SQS hoặc SSE-KMS)
- IAM **least privilege** cho producer / consumer
- Queue policy cẩn thận khi cho phép cross-account access

### 5.7 Thứ tự xử lý và scale consumer

- **Standard**: scale ngang nhiều consumer để tăng throughput
- **FIFO**: bị ràng buộc bởi message groups (mỗi group xử lý tuần tự)
- Tránh phụ thuộc ordering nếu nghiệp vụ không bắt buộc

### 5.8 Chiến lược retry có chủ đích

- Retry nhanh cho lỗi tạm thời (transient)
- Exponential backoff + jitter để tránh thundering herd
- Giới hạn số lần retry, sau đó đẩy qua DLQ

### 5.9 Kích thước message và thiết kế payload

- Message tối đa **256 KB**
- Payload lớn: lưu object trên **S3**, queue chỉ gửi pointer / metadata
- Thêm **schema version** để dễ evolve định dạng message

### 5.10 Nhận thức về trùng lặp và thứ tự

- Không giả định mỗi message chỉ xuất hiện **một lần**
- Không giả định receive **đúng thứ tự** (với Standard)
- Logic consumer phải chịu được reorder / duplicate

---

## 6. Checklist triển khai nhanh

- [ ] Chọn loại queue: Standard hay FIFO
- [ ] Đặt Visibility Timeout phù hợp workload
- [ ] Bật long polling
- [ ] Cấu hình DLQ + `maxReceiveCount`
- [ ] Thiết kế consumer idempotent
- [ ] Bật monitoring + alert backlog / DLQ
- [ ] Bật encryption + IAM least privilege
- [ ] Viết runbook: replay DLQ, scale consumer, xử lý sự cố

---

## 7. Lỗi thường gặp

- Quên `DeleteMessage` sau khi xử lý thành công
- Để visibility timeout mặc định, không theo workload
- Không có DLQ hoặc có DLQ nhưng không theo dõi
- Coi SQS Standard là exactly-once
- Retry vô hạn → tăng chi phí và nghẽn hệ thống

---

## 8. Tóm tắt

SQS rất phù hợp cho xử lý bất đồng bộ, giải coupling và hấp thụ đỉnh tải.

Để hệ thống ổn định trên production, tập trung **bốn điểm**: **idempotency**, **visibility timeout**, **chiến lược DLQ**, và **observability**.

---

## 9. SQS có gì đặc biệt?

So với nhiều queue system khác, SQS nổi bật ở các điểm sau:

- **Managed hoàn toàn**: không phải tự vận hành broker, cluster, patch hay capacity planning phức tạp.
- **Độ bền cao + scale tự động**: phù hợp workload tăng/giảm thất thường.
- **Tách producer/consumer tốt**: hệ thống chịu tải đột biến tốt hơn nhờ cơ chế đệm.
- **Tích hợp sâu với AWS**: Lambda, SNS, EventBridge, IAM, CloudWatch, KMS.
- **Có cả Standard và FIFO**: linh hoạt giữa throughput cao và yêu cầu ordering/dedup.
- **Chi phí theo mức dùng**: dễ bắt đầu, tối ưu bằng batch + long polling.

---

## 10. Giải thích các thông số trên màn hình queue của bạn

Ảnh bạn gửi đang ở queue `orch-dev-deal-events` (loại **Standard**) trong AWS Console.

### 10.1 Khối Details

- **Name**  
  Tên queue. Nếu là FIFO thì tên phải kết thúc bằng `.fifo`; queue của bạn là Standard nên không cần.

- **Type: Standard**  
  Loại queue hiện tại. Ý nghĩa:
  - throughput cao
  - có thể nhận trùng
  - không đảm bảo thứ tự tuyệt đối

- **Encryption: Amazon SQS key (SSE-SQS)**  
  Queue đang bật mã hóa server-side bằng khóa managed mặc định của SQS.  
  Nếu cần kiểm soát key chặt hơn (audit/rotation/policy), có thể dùng **SSE-KMS** với CMK.

- **URL**  
  Endpoint logic của queue, dùng trong SDK/API (`SendMessage`, `ReceiveMessage`, `DeleteMessage`...).

- **ARN**  
  Định danh tài nguyên AWS duy nhất của queue, dùng trong IAM policy, queue policy, tích hợp service khác.

- **Dead-letter queue: `flattest`**  
  Queue này đã nối với một DLQ tên `flattest`. Message lỗi vượt ngưỡng receive sẽ bị chuyển sang đây.

### 10.2 Các nút thao tác phía trên

- **Edit**: sửa cấu hình queue.
- **Delete**: xóa queue.
- **Purge**: xóa toàn bộ message trong queue hiện tại (cần cẩn trọng).
- **Send and receive messages**: test thủ công gửi/nhận/xóa message ngay trên console.
- **Start DLQ redrive**: đẩy message từ DLQ quay lại queue nguồn để xử lý lại.

### 10.3 Các tab cấu hình bạn đang thấy

- **Queue policies**  
  Chính sách truy cập resource-level cho queue.  
  Ảnh của bạn đang báo **No access policy for this queue**, nghĩa là chưa có policy custom (chỉ IAM identity-based có thể đang quyết định quyền truy cập).

- **Monitoring**  
  Xem metric CloudWatch: backlog, oldest message age, throughput, số message nhận/xóa...

- **SNS subscriptions**  
  Gắn queue làm subscriber của SNS topic.

- **Lambda triggers**  
  Nối queue với Lambda event source mapping để Lambda tự poll và xử lý message.

- **EventBridge Pipes**  
  Thiết lập pipeline source/target có filter/enrichment giữa SQS và dịch vụ đích.

- **Dead-letter queue**  
  Cấu hình liên kết DLQ và chính sách redrive (`maxReceiveCount`).

- **Tagging**  
  Gắn tag cho cost allocation, quản trị môi trường (dev/stg/prod), ownership.

- **Encryption**  
  Chọn SSE-SQS hoặc SSE-KMS và thông số liên quan mã hóa.

- **Dead-letter queue redrive tasks**  
  Quản lý lịch sử/trạng thái các tác vụ redrive từ DLQ.

### 10.4 Hai mục ngay dưới tab Queue policies

- **Access policy**  
  Xác định principal nào (account/service/user/role) được thao tác với queue ở cấp tài nguyên.

- **Redrive allow policy**  
  Quy định queue nào được phép dùng queue hiện tại làm DLQ (hạn chế nhầm lẫn/misuse giữa nhiều queue).

### 10.5 Gợi ý nhanh cho queue hiện tại

- Giữ **Standard** nếu nghiệp vụ không bắt buộc strict ordering.
- Xác nhận `maxReceiveCount` hợp lý để không đẩy message sang DLQ quá sớm.
- Bật alert cho:
  - `ApproximateNumberOfMessagesVisible`
  - `ApproximateAgeOfOldestMessage`
  - số message vào DLQ
- Nếu có cross-account hoặc service publish trực tiếp, nên cấu hình rõ **Queue policy** thay vì để trống.

---

## 11. Giải thích màn hình "Send and receive messages"

Đây là màn hình test nhanh ngay trên AWS Console để bạn gửi/nhận message thủ công mà không cần code.

### 11.1 Phần Send message

- **Message body**  
  Nội dung chính của message. Thực tế nên gửi JSON để consumer parse dễ hơn.

- **Message group ID (optional, new)**  
  Trường này bắt buộc với FIFO; với Standard thì dùng cho tính năng *fair queue/noisy neighbor mitigation* (phân phối công bằng hơn giữa các nhóm message).

- **Delivery delay**  
  Trì hoãn lúc message bắt đầu được nhìn thấy bởi consumer.  
  Ví dụ đặt `30` giây thì trong 30 giây đầu message chưa thể receive.

- **Message attributes (optional)**  
  Metadata dạng key-value (String/Number/Binary), thường dùng cho filter, định tuyến hoặc tracing.

- **Clear content / Send message**  
  - `Clear content`: xóa nội dung form đang nhập.  
  - `Send message`: gửi message vào queue.

### 11.2 Phần Receive messages

- **Messages available**  
  Số message đang sẵn sàng để nhận (xấp xỉ).

- **Polling duration**  
  Thời gian long polling mỗi lần nhận message (ví dụ ảnh của bạn là `30s`).

- **Maximum message count**  
  Số message tối đa trả về trong một lần poll (tối đa 10 với 1 request).

- **Poll for messages / Stop polling**  
  - `Poll for messages`: bắt đầu poll queue.  
  - `Stop polling`: dừng poll.

- **Polling progress**  
  Hiển thị tiến trình của phiên poll hiện tại.

- **Messages (0)**  
  Danh sách message nhận được trong phiên hiện tại.

- **View details**  
  Xem đầy đủ `MessageId`, body, attributes, `ReceiptHandle`, timestamp...

- **Delete**  
  Xóa message đã receive (thực chất gọi `DeleteMessage` bằng `ReceiptHandle`).

### 11.3 Lưu ý quan trọng khi test ở màn hình này

- Receive xong nhưng **không bấm Delete**, message sẽ xuất hiện lại sau `Visibility Timeout`.
- `Messages available = 0` chưa chắc hệ thống "không có message" tuyệt đối vì đây là chỉ số xấp xỉ.
- Poll có thể trả về rỗng nếu chưa có message trong khoảng long polling.
- Nếu message không tới consumer:
  - kiểm tra `Delivery delay`
  - kiểm tra `Visibility Timeout`
  - kiểm tra message có bị đẩy sang DLQ không
  - kiểm tra quyền IAM/Queue policy

### 11.4 Quy trình test nhanh đề xuất

1. Gửi 1 message JSON từ phần **Send message**.
2. Bấm **Poll for messages** để nhận.
3. Mở **View details** để kiểm tra body/attributes.
4. Bấm **Delete** để xác nhận luồng xử lý thành công.
5. Nếu muốn test retry/duplicate: nhận message nhưng **không delete**, chờ hết visibility timeout rồi poll lại.
