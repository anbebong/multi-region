# multi-region

Framework Go cho dịch vụ log/config phân cấp: một `node.Node` có thể vừa là
**Trung tâm** (nhận log, phân phối cấu hình xuống con), vừa là **Chi nhánh**
(vừa nhận từ con của nó, vừa gửi lên cấp trên), tùy vào cách bạn cấu hình —
không có khái niệm "role" cố định, khả năng của node hoàn toàn do các option
được truyền vào quyết định.

## Mục lục

- [Ý tưởng cốt lõi](#ý-tưởng-cốt-lõi)
- [Cấu trúc package](#cấu-trúc-package)
- [Cài đặt & build](#cài-đặt--build)
- [Chạy thử nhanh với binary `cmd/node`](#chạy-thử-nhanh-với-binary-cmdnode)
- [Tham chiếu file config JSON](#tham-chiếu-file-config-json)
- [Dùng như thư viện Go trong code của bạn](#dùng-như-thư-viện-go-trong-code-của-bạn)
- [Bật mTLS](#bật-mtls)
- [Chạy test](#chạy-test)
- [Sinh lại code từ proto](#sinh-lại-code-từ-proto)
- [Xử lý sự cố](#xử-lý-sự-cố)

## Ý tưởng cốt lõi

Một `Node` có 2 khả năng độc lập, bật/tắt bằng option khi khởi tạo:

| Option đã set | Node lắng nghe con? | Node nối lên cha? | Vai trò tương ứng |
|---|---|---|---|
| chỉ `WithListenAddr` | Có | Không | **Trung tâm** (gốc cây) |
| chỉ `WithResolver` | Không | Có | **Leaf** (lá cây, không có con) |
| cả hai | Có | Có | **Chi nhánh** (node trung gian) |

Luồng dữ liệu:

- **Log**: Con → Cha. Khi 1 log được `Ingest` (do agent gửi vào, hoặc do
  con của node gửi lên), node đó **lưu local** (qua `Storage`) rồi **forward
  tiếp lên cha** (nếu có). Lặp lại tới khi tới node gốc.
- **Config**: Cha → Con. Khi node gốc gọi `Distribute`, config được đẩy
  xuống mọi con đang kết nối; mỗi con nhận xong lại tự `Distribute` tiếp
  xuống con của nó — lan truyền đệ quy xuống toàn cây.
- **Chịu lỗi**: nếu mất kết nối lên cha, log vẫn nằm an toàn trong storage
  local; 1 vòng lặp nền (`flushLoop`) định kỳ thử gửi lại, và kết nối gRPC
  tự mở lại stream mới khi cha online trở lại — không cần khởi động lại
  tiến trình.

## Cấu trúc package

```
multi-region/
├── node/          # core: Node, Option, Start/Stop/Ingest
├── transport/      # gRPC bidirectional stream server + client (mTLS)
├── proto/          # định nghĩa protobuf (LogEntry, ConfigPayload...)
├── storage/        # interface Storage + BoltDB mặc định
├── forwarder/       # gửi log lên cha, có retry/backoff
├── configmgr/       # phân phối config xuống cây con
├── resolver/        # tìm địa chỉ cha (mặc định: static config)
├── auth/           # mTLS Authenticator + helper sinh cert test
└── cmd/
    ├── node/       # binary tham khảo: đọc file JSON, chạy 1 node thật
    └── checkdb/    # tool nhỏ để đọc thử 1 file BoltDB (debug/kiểm tra)
```

## Cài đặt & build

Yêu cầu: Go 1.22+ (đã test với `go1.25` qua toolchain tự động), và `protoc`
nếu bạn cần sửa `proto/node.proto`.

```bash
go build ./...              # build toàn bộ package + binary
go build -o bin/node.exe ./cmd/node   # build riêng binary chạy thử (Windows)
```

## Chạy thử nhanh với binary `cmd/node`

Ví dụ dựng 2 node độc lập: 1 Trung tâm (`root`) + 1 Chi nhánh (`branch-1`)
nối lên `root`. Đã có sẵn 2 file mẫu:

- `cmd/node/config.example.root.json`
- `cmd/node/config.example.branch.json`

**Bước 1 — build binary:**

```bash
go build -o bin/node.exe ./cmd/node
```

**Bước 2 — chạy 2 tiến trình ĐỘC LẬP** (mở 2 cửa sổ terminal khác nhau,
không chạy chung 1 shell bằng `&` để tránh nhầm lẫn tiến trình):

Terminal 1 (Trung tâm):
```powershell
.\bin\node.exe .\cmd\node\config.example.root.json
```

Terminal 2 (Chi nhánh):
```powershell
.\bin\node.exe .\cmd\node\config.example.branch.json
```

Log khởi động sẽ in ra dạng:
```
node "root" started (listen="127.0.0.1:9443" parent="")
node "branch-1" started (listen="127.0.0.1:9444" parent="127.0.0.1:9443")
```

**Bước 3 — gửi thử 1 log entry vào Chi nhánh** (mỗi config có sẵn
`http_addr` để nhận log test qua HTTP, không cần viết code Go):

```bash
curl -X POST http://127.0.0.1:8081/ingest -d '{"payload":"hello from branch"}'
```

**Bước 4 — xác nhận log đã trôi lên Trung tâm.** Dừng cả 2 tiến trình
(Ctrl+C hoặc `Stop-Process`) để giải phóng khóa file BoltDB, rồi đọc thử
bằng tool `checkdb`:

```bash
go run ./cmd/checkdb root.db
# id=branch-1-...  node=branch-1  payload=hello from branch
# total=1
```

Muốn thêm 1 tầng Leaf nối vào `branch-1`, tạo thêm 1 file config chỉ có
`parent_addr: "127.0.0.1:9444"` (không có `listen_addr`), chạy tiến trình
thứ 3, rồi gửi log vào `http_addr` của Leaf đó — log sẽ trôi qua Chi nhánh
rồi lên tới Trung tâm.

## Tham chiếu file config JSON

File config của binary `cmd/node` (xem `cmd/node/config.go`):

| Trường | Bắt buộc? | Ý nghĩa |
|---|---|---|
| `id` | Có | Định danh node, gắn vào `NodeId` của mọi log do node này `Ingest`. |
| `listen_addr` | Ít nhất 1 trong 2 với `parent_addr` | Địa chỉ TCP node lắng nghe con (vd `"127.0.0.1:9443"`). Có → node nhận log/phân phối config xuống con. |
| `parent_addr` | Ít nhất 1 trong 2 với `listen_addr` | Địa chỉ TCP của node cha. Có → node tự kết nối lên cha khi start. |
| `storage_path` | Có | Đường dẫn file BoltDB lưu log local của node này. |
| `http_addr` | Không | Nếu set, mở thêm 1 HTTP server nội bộ với `POST /ingest {"payload": "..."}` — chỉ để test tay, không phải giao thức chính thức giữa các node. |
| `tls` | Không | Bật mTLS thật (xem phần dưới). Bỏ trống thì chạy gRPC insecure — **chỉ dùng để thử nghiệm local, không dùng khi triển khai thật.** |

Ví dụ đầy đủ (có mTLS):
```json
{
  "id": "branch-1",
  "listen_addr": "127.0.0.1:9444",
  "parent_addr": "127.0.0.1:9443",
  "storage_path": "./branch.db",
  "http_addr": "127.0.0.1:8081",
  "tls": {
    "ca_cert_path": "./certs/ca.pem",
    "cert_path": "./certs/branch-1.pem",
    "key_path": "./certs/branch-1.key"
  }
}
```

## Dùng như thư viện Go trong code của bạn

`cmd/node` chỉ là 1 chương trình tham khảo minh họa cách gọi thư viện — bạn
có thể import trực tiếp các package và tự viết logic riêng:

```go
import (
    "context"
    "time"

    "github.com/lancsnet/multi-region/auth"
    "github.com/lancsnet/multi-region/node"
    "github.com/lancsnet/multi-region/proto"
    "github.com/lancsnet/multi-region/resolver"
    "github.com/lancsnet/multi-region/storage"
)

func main() {
    store, _ := storage.NewBoltStorage("./branch.db")
    authn, _ := auth.NewMTLSAuthenticator("ca.pem", "branch-1.pem", "branch-1.key")

    n, err := node.New(
        node.WithID("branch-1"),
        node.WithListenAddr(":9443"),                                   // có con
        node.WithResolver(resolver.NewStaticResolver("root:9443")),      // có cha
        node.WithStorage(store),
        node.WithAuthenticator(authn),
    )
    if err != nil {
        panic(err)
    }

    ctx := context.Background()
    if err := n.Start(ctx); err != nil {
        panic(err)
    }
    defer n.Stop()

    n.Ingest(ctx, &proto.LogEntry{
        Id:        "1",
        NodeId:    "branch-1",
        Timestamp: time.Now().Unix(),
        Payload:   []byte("hello"),
    })
}
```

Muốn thay storage backend (Postgres, Elasticsearch...) thì tự implement
interface `storage.Storage` và truyền vào `node.WithStorage(...)` — không
cần sửa gì trong `node`/`transport`.

## Bật mTLS

Mỗi node cần 1 bộ chứng chỉ ký bởi cùng 1 CA nội bộ:

- `ca_cert_path`: chứng chỉ CA dùng để xác thực chứng chỉ của phía đối diện.
- `cert_path` / `key_path`: chứng chỉ + private key riêng của node đó.

Package `auth` cung cấp `auth.GenerateTestCA(t)` để sinh CA + cert test
nhanh trong unit test (xem `auth/testutil.go`) — dùng để tham khảo cách
tạo cert, **không dùng file đó cho production**. Khi triển khai thật, sinh
CA/cert bằng công cụ PKI nội bộ của bạn (Vault, cfssl, openssl...) rồi trỏ
`tls` trong config JSON tới các file đó.

Nếu bỏ trống `tls`, `cmd/node` sẽ in cảnh báo và chạy gRPC không mã hóa —
chỉ nên dùng khi thử nghiệm trên máy local.

## Chạy test

```bash
go test ./...            # toàn bộ unit test + integration test
go test ./node/... -v    # riêng test tích hợp nhiều node (3 tầng, resilience)
```

Test tích hợp trong `node/integration_test.go` và
`node/resilience_test.go` dựng thật 2-3 node qua TCP `127.0.0.1` (không
mock), bao gồm cả kịch bản mất kết nối cha rồi phục hồi.

## Sinh lại code từ proto

Nếu bạn sửa `proto/node.proto`, cần cài `protoc` + 2 plugin rồi generate
lại:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
make proto
```

## Xử lý sự cố

- **"resolver: no parent address configured"**: node được tạo với
  `WithResolver` nhưng địa chỉ rỗng — kiểm tra `parent_addr` trong config.
- **`node: at least one of WithListenAddr ... or WithResolver ... is
  required`**: node không có cả `listen_addr` lẫn `parent_addr` — 1 node
  vô dụng (không nhận ai, không gửi cho ai) không được phép khởi tạo.
- **BoltDB báo "timeout" khi mở file đang chạy**: file `.db` đang bị khóa
  bởi 1 tiến trình `node.exe` khác đang chạy trên cùng file đó (bbolt dùng
  file lock độc quyền). Dừng tiến trình đó trước khi dùng `cmd/checkdb`
  hoặc mở lại bằng process khác.
- **Log không thấy trôi lên cha khi test tay**: kiểm tra `flushLoop` chạy
  mỗi 2 giây — đợi vài giây rồi kiểm tra lại; nếu vẫn không thấy, kiểm tra
  `parent_addr` có đúng địa chỉ `listen_addr` của cha không, và cha có
  đang thực sự lắng nghe (xem log `ingest endpoint listening...`).

See `docs/superpowers/specs/2026-07-17-hierarchical-node-framework-design.md`
for kiến trúc/thiết kế gốc.
