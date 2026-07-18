# BA: Yêu cầu mở rộng quy mô — nhiều node, nhiều lớp chi nhánh

Ngày: 2026-07-18

## 1. Bối cảnh

Framework hiện tại (`docs/superpowers/specs/2026-07-17-hierarchical-node-framework-design.md`)
được thiết kế và test ở quy mô nhỏ: 2-3 node, 1 agent, throughput thấp
(curl thủ công / demo). Người dùng xác nhận quy mô thực tế nhắm tới lớn
hơn nhiều:

| Tiêu chí | Yêu cầu thực tế |
|---|---|
| Throughput | Cao — streaming liên tục từ mỗi agent, không phải log rời rạc |
| Lưu trữ | Mọi tầng (root, mọi branch, mọi leaf) đều giữ lịch sử log riêng làm audit cục bộ — không chỉ root |
| Cấu trúc cây | 4+ tầng sâu, fan-out lớn — hàng trăm con/nút ở một số tầng |
| Số node | Hàng trăm node, hàng nghìn agent trở lên |

Tài liệu này liệt kê từng điểm trong kiến trúc hiện tại **sẽ không chịu
được** quy mô này, kèm mức độ nghiêm trọng và hướng khắc phục đề xuất.
Mục đích: thống nhất phạm vi trước khi sửa code.

## 2. Các điểm nghẽn theo kiến trúc hiện tại

### 2.1. `flushLoop` — poll cố định 2s, quét toàn bộ storage (nghiêm trọng)

`node/node.go` — mỗi node có `resolver` (mọi branch + leaf) chạy 1
goroutine `flushLoop`, cứ 2 giây lại `Query` **toàn bộ** log thuộc về
node đó rồi forward lại toàn bộ, bất kể đã gửi thành công hay chưa
(bug đang sửa dở ở phiên trước — cần `OnlyUndelivered` filter làm nền
tảng tối thiểu).

Ở quy mô hàng trăm node, hàng nghìn agent:
- Mỗi node tự poll độc lập, không đồng bộ — tạo hàng trăm truy vấn
  BoltDB đồng thời mỗi 2 giây trên toàn hệ thống, kể cả khi không có
  gì mới cần gửi.
- Chu kỳ cố định 2s không co giãn theo tải: log đến dồn dập (streaming)
  sẽ tích tụ giữa 2 lần poll, gây delay không cần thiết; lúc rảnh thì
  lãng phí quét rỗng.

**Đề xuất**: chuyển từ poll cố định sang mô hình event-driven —
forward ngay khi log đến (đã có), `flushLoop` chỉ đóng vai trò an toàn
dự phòng (backoff tăng dần khi liên tục thất bại, thay vì quét đều 2s
bất kể trạng thái kết nối cha).

### 2.2. BoltDB đơn file, mỗi node 1 file — không có index truy vấn (nghiêm trọng ở audit mọi tầng)

`storage/bolt.go` — `Query()` duyệt tuần tự (`ForEach`) toàn bộ bucket,
lọc bằng vòng lặp Go, không có index theo `node_id`/`timestamp`.

Khi **mọi tầng đều giữ lịch sử log riêng** (yêu cầu vừa xác nhận) và
throughput cao, file `.db` ở mỗi branch sẽ phình rất nhanh và không có
cơ chế:
- Xoay vòng / nén / archive log cũ.
- Index để query nhanh theo thời gian hoặc theo node con — mọi truy vấn
  admin (`GET /api/v1/admin/logs`) sẽ ngày càng chậm khi data lớn lên.
- Giới hạn dung lượng — không có retention policy, log tồn tại vĩnh viễn.

**Đề xuất**: cần quyết định retention (giữ bao lâu ở mỗi tầng), có thể
cần thay executor BoltDB bằng engine có index thật (hoặc thêm lớp index
phụ trong Bolt) tùy khối lượng log cụ thể.

### 2.3. `transport.Server.children` — map con chỉ đếm số lượng, không định danh (trung bình, nhưng chặn quan sát ở quy mô lớn)

`transport/server.go` — mỗi con connect chỉ được gán 1 `int64` tăng dần
nội bộ, không map với ID logic (hostname/node_id) của con đó.

Với fan-out hàng trăm con/nút, khi có sự cố (1 branch ngừng gửi log),
hiện tại **không thể biết chính xác con nào** đang có vấn đề — chỉ biết
tổng số con đang connect. Đây là yêu cầu quan sát/giám sát tối thiểu ở
quy mô lớn.

**Đề xuất**: con phải tự giới thiệu ID khi mở stream (cần sửa
`proto/node.proto` — đã có `protoc` cài xong, sẵn sàng làm việc này),
để cha track theo ID logic, không chỉ đếm số.

### 2.4. Một stream gRPC riêng cho mỗi log entry gửi lẻ — không batch (nghiêm trọng nếu throughput cao)

`transport/client.go SendLog` gửi từng `LogEntry` một qua
`stream.Send`. Với agent streaming liên tục và fan-out lớn, số lượng
message nhỏ lẻ dồn lên mỗi tầng cha sẽ rất lớn (mỗi tầng cộng dồn
throughput của toàn bộ cây con phía dưới nó).

**Đề xuất**: cân nhắc gộp batch (N entries/khoảng thời gian ngắn) trước
khi gửi lên cha, giảm số round-trip và overhead protobuf/gRPC framing.

### 2.5. Không có backpressure — `Server.Broadcast` drop config khi buffer đầy (thấp, nhưng cần biết)

`transport/server.go Broadcast`: kênh gửi config tới mỗi con có buffer
16, nếu đầy thì **âm thầm drop** (`default:` case). Ở fan-out lớn, việc
push config có thể mất mà không báo lỗi rõ ràng lên tầng gọi.

**Đề xuất**: mức độ ưu tiên thấp hơn 4 mục trên, nhưng cần quyết định:
chấp nhận rủi ro mất config broadcast, hay cần cơ chế xác nhận
(ack)/retry riêng cho config (khác với log, vốn đã có retry).

## 3. Câu hỏi cần chốt trước khi thiết kế lại

1. **Retention log ở mỗi tầng**: giữ bao lâu / bao nhiêu dung lượng ở
   branch trước khi cần archive hoặc xóa? Không giới hạn sẽ không chịu
   được throughput cao lâu dài.
2. **Định danh con**: có chấp nhận sửa `proto/node.proto` để con tự
   gửi ID logic khi connect không? (Cần dùng `protoc` vừa cài.)
3. **Batching log**: có chấp nhận độ trễ nhỏ (gộp theo khoảng thời gian
   ngắn, ví dụ 100ms–1s) để đổi lấy giảm tải mạng/CPU đáng kể ở quy mô
   lớn không, hay cần độ trễ tối thiểu tuyệt đối (gửi ngay từng entry)?
4. **Giám sát**: ngoài log ra terminal (đã có), có cần export metrics
   (Prometheus) cho throughput/children_count/lag mỗi tầng không, vì ở
   quy mô hàng trăm node, chỉ đọc log terminal sẽ không đủ để vận hành?

## 4. Quyết định đã chốt (2026-07-18)

| Vấn đề | Quyết định |
|---|---|
| Retention log mỗi tầng | Có thời hạn (N ngày / N GB), tự động dọn log cũ ở mọi node |
| Định danh con khi kết nối | Sửa `proto/node.proto`, con tự gửi `node_id` logic khi mở stream — cha track theo ID thật, không chỉ đếm số |
| Batching log lên cha | Chấp nhận độ trễ nhỏ (gom theo lô, ví dụ vài trăm ms hoặc N entries) để giảm tải mạng/CPU |
| Giám sát | Cần export metrics chuẩn Prometheus (throughput, children_count, lag...) mỗi tầng, không chỉ log terminal |

Ý nghĩa với kiến trúc: đây là thay đổi non-trivial chạm tới cả 5 package
cốt lõi (`proto`, `transport`, `node`, `storage`, và một package mới
cho metrics) — cần thiết kế chi tiết (kèm sơ đồ interface/message proto
mới) trước khi code, không sửa vá từng phần rời rạc.

## 5. Việc tiếp theo

1. Hoàn tất delivery-tracking đang dở (flushLoop không gửi lặp log đã
   `MarkDelivered`) — nền tảng đúng ở mọi quy mô, làm trước, độc lập
   với các quyết định ở mục 4.
2. Thiết kế chi tiết (interface, message proto mới, package layout) cho
   4 hạng mục đã chốt — sẽ lập kế hoạch triển khai riêng, dùng chế độ
   Plan, trước khi bắt đầu sửa code lớn.
