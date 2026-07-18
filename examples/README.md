# Examples

Các binary trong thư mục này minh họa cách dùng framework `multi-region`
(xem [README.md ở gốc repo](../README.md) để hiểu framework trước). Mỗi ví
dụ tự định nghĩa nội dung nghiệp vụ riêng (log/config), tự chọn cách lưu
trữ, tự viết REST API — framework core không biết gì về những thứ này.

| Thư mục | Vai trò |
|---|---|
| `node/` | Binary chính: 1 service hoàn chỉnh dùng framework — REST API, dashboard, allow-list, storage. Chạy được như root/branch/leaf tùy config. |
| `agent/` | Binary mô phỏng agent trên PC, gửi log định kỳ vào `node/` qua REST. |
| `gencert/` | Tool sinh CA + cert tự ký, dùng để chạy thử mTLS mà không cần cài openssl/cfssl. |
| `checkdb/` | Tool debug nhỏ, đọc thử file BoltDB mà `node/` dùng để lưu trữ. |

## Mục lục

- [Chạy thử nhanh (3 node, mTLS, phê duyệt)](#chạy-thử-nhanh-3-node-mtls-phê-duyệt)
- [Thử cơ chế phê duyệt qua dashboard](#thử-cơ-chế-phê-duyệt-qua-dashboard)
- [Tham chiếu file config JSON của `node/`](#tham-chiếu-file-config-json-của-node)
- [REST API của `node/`](#rest-api-của-node)
- [Xử lý sự cố](#xử-lý-sự-cố)

## Chạy thử nhanh (3 node, mTLS, phê duyệt)

Có sẵn đúng 1 bộ 3 file config trong `node/`, luôn dùng mTLS, đặt tên theo
mô hình hành chính 3 cấp: `config.example.trunguong.json` (**TRUNG-UONG**,
gốc cây), `config.example.tinh.json` (**TINH**, tầng giữa),
`config.example.xa.json` (**XA**, lá cây) — dựng cây 3 tầng
TRUNG-UONG → TINH → XA.

**Bước 1 — build các binary** (chạy từ thư mục gốc repo):

```bash
go build -o bin/node.exe ./examples/node
go build -o bin/agent.exe ./examples/agent
```

**Bước 2 — sinh CA + cert mẫu** (mỗi node 1 cert riêng, không cần cài
openssl/cfssl):

```bash
go run ./examples/gencert -out examples/node/certs TRUNG-UONG TINH XA
```

Lệnh này tạo ra đúng các file mà 3 config mẫu đang trỏ tới
(`examples/node/certs/ca.pem`, `TRUNG-UONG.pem`/`.key`, v.v.). **Không
commit thư mục `certs/` này** — nó đã nằm trong `.gitignore`.

**Bước 3 — chạy 3 tiến trình ĐỘC LẬP, đúng thứ tự** (mỗi lệnh 1 cửa sổ
terminal riêng, không chạy chung 1 shell bằng `&`):

Terminal 1 (TRUNG-UONG):
```powershell
.\bin\node.exe .\examples\node\config.example.trunguong.json
```

Terminal 2 (TINH):
```powershell
.\bin\node.exe .\examples\node\config.example.tinh.json
```

Terminal 3 (XA):
```powershell
.\bin\node.exe .\examples\node\config.example.xa.json
```

Vì 2 file config đầu đã có sẵn `allowed_child_ids`, bạn sẽ thấy log
duyệt tự động, không cần thao tác gì thêm:
```
[admin] approved node-id "TINH" to connect as a child   (log ở TRUNG-UONG)
[admin] approved node-id "XA" to connect as a child      (log ở TINH)
```

**Bước 4 — gửi log**, qua agent ví dụ (gửi định kỳ):

```powershell
.\bin\agent.exe .\examples\agent\config.example.json
```

hoặc gửi tay 1 lần bằng curl (vào TINH, vì file mẫu của agent trỏ tới
`http://127.0.0.1:8081` — đổi `service_addr` trong
`examples/agent/config.example.json` nếu muốn gửi vào TRUNG-UONG `:8080`
hoặc XA `:8082`):
```bash
curl -X POST http://127.0.0.1:8081/api/v1/agent/logs -d '{"payload":"hello"}'
```

**Bước 5 — mở dashboard** tại `http://127.0.0.1:8080/` (TRUNG-UONG),
`http://127.0.0.1:8081/` (TINH), `http://127.0.0.1:8082/` (XA) để xem
status, log cục bộ, đẩy config, và quản lý danh sách phê duyệt con.

**Bước 6 — xác nhận log đã trôi lên TRUNG-UONG.** Dừng cả 3 tiến trình
(Ctrl+C) để giải phóng khóa file BoltDB, rồi đọc thử bằng tool `checkdb`:

```bash
go build -o bin/checkdb.exe ./examples/checkdb
./bin/checkdb.exe trunguong.db
# id=TINH-...  kind=log  payload=hello
# total=1
```

## Thử cơ chế phê duyệt qua dashboard

Muốn thấy rõ hơn cơ chế phê duyệt (không chỉ đọc sẵn từ file), xóa dòng
`"allowed_child_ids": ["TINH"]` khỏi `config.example.trunguong.json` rồi
khởi động lại TRUNG-UONG + TINH. TINH sẽ bị từ chối liên tục:
```
[transport] rejected child connection (node-id="TINH"): node-id "TINH" is not in the allowed list
```
Mở `http://127.0.0.1:8080/`, mục "Phê duyệt con kết nối", gõ `TINH` rồi
bấm "Phê duyệt" — trong vài giây, TINH tự kết nối lại thành công mà
**không cần restart tiến trình TINH** (đây là cơ chế reconnect tự động
của framework — xem phần "Phê duyệt con kết nối" trong
[README.md gốc](../README.md#phê-duyệt-con-kết-nối)).

## Tham chiếu file config JSON của `node/`

File config của binary `node/` (xem `node/config.go`):

| Trường | Bắt buộc? | Ý nghĩa |
|---|---|---|
| `id` | Có | Tên bạn tự đặt cho node này — cũng là `node-id` nó tự khai báo khi kết nối lên cha. |
| `listen_addr` | Ít nhất 1 trong 2 với `parent_addr` | Địa chỉ TCP node lắng nghe con (vd `"127.0.0.1:9443"`). Có → node nhận Envelope từ con/phân phối downstream xuống con. |
| `parent_addr` | Ít nhất 1 trong 2 với `listen_addr` | Địa chỉ TCP của node cha. Có → node tự kết nối lên cha khi start. |
| `storage_path` | Có | Đường dẫn file BoltDB — nơi *service ví dụ này* lưu bản ghi Envelope của riêng nó (không phải framework). |
| `http_addr` | Không | Nếu set, mở thêm 1 HTTP server nội bộ với REST API + dashboard tại `/` (xem bảng bên dưới). |
| `tls` | Không | Bật mTLS thật — cần `ca_cert_path`/`cert_path`/`key_path`. Bỏ trống thì chạy gRPC insecure — **chỉ dùng để thử nghiệm local, không dùng khi triển khai thật.** |
| `allowed_child_ids` | Không | Danh sách `node-id` được phép kết nối làm con **lúc khởi động** — có thể thêm/xóa sau đó qua API/dashboard mà không cần sửa file này hay restart. Chỉ có tác dụng khi `tls` được bật. Bỏ trống = chưa duyệt sẵn ai. |

Ví dụ đầy đủ (`config.example.tinh.json`):
```json
{
  "id": "TINH",
  "listen_addr": "127.0.0.1:9444",
  "parent_addr": "127.0.0.1:9443",
  "storage_path": "./tinh.db",
  "http_addr": "127.0.0.1:8081",
  "tls": {
    "ca_cert_path": "./examples/node/certs/ca.pem",
    "cert_path": "./examples/node/certs/TINH.pem",
    "key_path": "./examples/node/certs/TINH.key"
  },
  "allowed_child_ids": ["XA"]
}
```

## REST API của `node/`

Đây là API do **ví dụ này tự định nghĩa** (không phải API của framework —
framework chỉ có các hàm Go như `SendUp`/`SendDown`/`OnUpstream`...):

| Endpoint | Chức năng |
|---|---|
| `POST /api/v1/agent/logs` | Agent gửi 1 dòng log (`{"payload": "..."}`) |
| `POST /api/v1/admin/config` | Đẩy config xuống **mọi** con đang kết nối |
| `POST /api/v1/admin/config/{child_id}` | Đẩy config xuống **đúng 1 con** |
| `GET /api/v1/admin/logs` | Xem log cục bộ node này đã lưu |
| `GET /api/v1/admin/status` | Trạng thái node: id, có cha/con không, đang kết nối không |
| `GET /api/v1/admin/children` | Số con đang kết nối |
| `GET /api/v1/admin/allowed-children` | Danh sách `node-id` đang được phép làm con |
| `POST /api/v1/admin/allowed-children` | Phê duyệt thêm 1 `node-id` (`{"node_id": "..."}`) |
| `DELETE /api/v1/admin/allowed-children/{node_id}` | Thu hồi phê duyệt |
| `GET /api/v1/admin/pending-children` | Danh sách `node-id` đang cố kết nối nhưng bị từ chối (chưa được duyệt) |
| `GET /metrics` | Số liệu Prometheus về cơ chế vận chuyển (xem [Metrics trong README gốc](../README.md#metrics-prometheus)) |
| `GET /` | Dashboard HTML — giao diện cho tất cả các endpoint trên |

Chính sách phê duyệt cụ thể (đối chiếu `node-id` với allow-list, xác nhận
khớp CommonName trên chứng chỉ mTLS) nằm trong `node/allowlist.go`
(`childAllowList`) — service ví dụ này tự viết, framework không biết gì
về việc này.

## Xử lý sự cố

- **"resolver: no parent address configured"**: node được tạo với
  `WithResolver` nhưng địa chỉ rỗng — kiểm tra `parent_addr` trong config.
- **`node: at least one of WithListenAddr ... or WithResolver ... is
  required`**: node không có cả `listen_addr` lẫn `parent_addr` — 1 node
  vô dụng (không nhận ai, không gửi cho ai) không được phép khởi tạo.
- **BoltDB báo "timeout" khi mở file đang chạy**: file `.db` đang bị khóa
  bởi 1 tiến trình `node.exe` khác đang chạy trên cùng file đó (bbolt dùng
  file lock độc quyền). Dừng tiến trình đó trước khi dùng `checkdb`
  hoặc mở lại bằng process khác.
- **Log không thấy trôi lên cha khi test tay**: kết nối mất tạm thời được
  giữ trong hàng đợi bộ nhớ và vòng lặp nền tự thử lại định kỳ; đợi vài
  giây rồi kiểm tra lại; nếu vẫn không thấy, kiểm tra `parent_addr` có
  đúng địa chỉ `listen_addr` của cha không, và cha có đang thực sự lắng
  nghe (xem log `listening for children on...`).
- **Con bị từ chối kết nối liên tục**: kiểm tra `allowed_child_ids` của
  cha (hoặc dashboard "Phê duyệt con kết nối") có chứa đúng `id` của con
  không, và `id` đó có khớp với CommonName trên chứng chỉ mTLS của con
  không (xem log `[transport] rejected child connection`).
- **TLS lỗi "doesn't contain any IP SANs"**: cert sinh thủ công thiếu
  IP SAN cho `127.0.0.1` — dùng `gencert` (đã có sẵn IP SAN) thay vì tự
  tạo cert bằng tay, hoặc thêm `IPAddresses` khi tự tạo.
