# Thiet ke: Go Framework cho Node phan cap (Trung tam / Chi nhanh)

Ngay: 2026-07-17

## 1. Muc tieu

Xay dung mot Go library (framework) cho phep tao ra cac "Node" co the dong thoi:
- **Vai tro Cha (Trung tam)**: nhan log tu cac Node con, quan ly va phan phoi cau hinh xuong con chau.
- **Vai tro Con (Chi nhanh)**: gui log len Node cha, nhan cau hinh tu cha va co the tiep tuc phan phoi xuong cac Node con cua chinh no.

Mot Node co the giu ca hai vai tro cung luc (Node trung gian trong cay phan cap), cho phep xay dung topology nhieu tang tuy y ma khong can 2 loai code/service rieng biet cho "server" va "agent".

Use case chinh: he thong thu thap log tap trung + quan ly cau hinh phan cap nhieu cap (vi du: Trung tam quan ly nhieu Vung/Chi nhanh, moi Vung lai quan ly nhieu Chi nhanh con hoac Leaf/agent).

## 2. Kien truc tong quan

```
        [Trung tam] (chi Server role)
         /        \
   [Chi nhanh A]  [Chi nhanh B]   (Server + Client role)
     /      \
 [Leaf1]  [Leaf2]                (chi Client role)
```

- **Chieu ket noi**: Node con luon la ben chu dong thiet lap ket noi len Node cha (phu hop khi con nam sau NAT/mang noi bo, cha co dia chi on dinh).
- **Giao thuc**: gRPC bidirectional streaming tren 1 ket noi ben vung. Con day log len lien tuc; cha push config xuong tren cung stream.
- **Xu ly log tai moi node**: Luu local (storage) + Forward tiep len cap tren. Moi tang trong cay deu co ban sao du lieu cua rieng no, Node goc (Trung tam) la noi hoi tu day du du lieu cuoi cung. Neu mat ket noi len cha, log van duoc giu trong storage local va retry gui lai khi ket noi phuc hoi (khong mat du lieu).
- **Phan phoi config**: Chi Node cha (co the la Trung tam hoac bat ky Node trung gian nao) push config xuong toan bo cay con cua no. Moi Node con nhan, ap dung local qua callback, roi tiep tuc phan phoi xuong con chau cua chinh no (truyen de quy theo cay).
- **Xac thuc**: mTLS (chung chi 2 chieu) qua CA chung cho tat ca ket noi giua cac tang.
- **Discovery dia chi cha**: Static config - moi Node con biet truoc dia chi 1 Node cha duy nhat qua file config, khong dung service registry dong.
- **Dong goi**: Framework la 1 Go module (library) de import vao service khac, khong phai standalone binary. Nguoi dung tu viet chuong trinh khoi tao `Node` voi cac option/role phu hop.

## 3. Cau truc module

```
multi-region/
├── node/          # core: Node struct, lifecycle (Start/Stop), quan ly vai tro
├── transport/     # gRPC server + client, bidirectional stream, mTLS, reconnect/retry
├── proto/         # protobuf: LogEntry, ConfigPayload, RegisterRequest...
├── storage/       # interface Storage{Save,Query,Delete} + default impl (BoltDB)
├── forwarder/     # interface Forwarder{Forward(LogEntry) error} - day log len cha
├── configmgr/     # interface ConfigDistributor - cha push config xuong toan bo cay con
├── resolver/      # interface Resolver{ParentAddr() string} - default: static config
└── auth/          # mTLS cert loader/validator, interface Authenticator
```

### Luong du lieu Log (Con -> Cha)

1. Nguon log (agent, ung dung...) goi `node.Ingest(ctx, logEntry)` tren Node con.
2. Node con: `storage.Save(logEntry)` (luu local) + `forwarder.Forward(logEntry)` (gui qua stream len cha).
3. Node cha nhan qua `transport` server, cung thuc hien `storage.Save` + tiep tuc `forwarder.Forward` len cha cua chinh no (neu co) - lap lai de quy cho toi Node goc (Trung tam).

### Luong Config (Cha -> Con)

1. Node cha goi `configmgr.Distribute(ctx, cfg)`.
2. Config duoc push xuong tat ca Node con dang connect (qua chieu server->client cua stream).
3. Moi Node con nhan, ap dung local qua callback `OnConfigUpdate`, roi tiep tuc `Distribute` xuong con chau cua chinh no.

## 4. Interface cot loi

```go
type Storage interface {
    Save(ctx context.Context, entry *LogEntry) error
    Query(ctx context.Context, filter QueryFilter) ([]*LogEntry, error)
    Delete(ctx context.Context, ids []string) error
}

type Forwarder interface {
    Forward(ctx context.Context, entry *LogEntry) error
    Close() error
}

type ConfigDistributor interface {
    Distribute(ctx context.Context, cfg *ConfigPayload) error
    OnConfigUpdate(handler func(*ConfigPayload)) // callback khi con nhan config
}

type Resolver interface {
    ParentAddr() (string, error)
}

type Authenticator interface {
    ClientTLSConfig() (*tls.Config, error)
    ServerTLSConfig() (*tls.Config, error)
}
```

### Node struct

```go
type Role int

const (
    RoleServer Role = iota // chi la Cha (Trung tam / goc cay)
    RoleClient              // chi la Con (Leaf)
    RoleBoth                 // vua Cha vua Con (Node trung gian)
)

type Node struct {
    ID        string
    Role      Role
    Storage   Storage
    Forwarder Forwarder
    Config    ConfigDistributor
    Resolver  Resolver
    Auth      Authenticator
}

func New(opts ...Option) (*Node, error)
func (n *Node) Start(ctx context.Context) error
func (n *Node) Stop() error
func (n *Node) Ingest(ctx context.Context, entry *LogEntry) error
```

Khoi tao qua Functional Options pattern: nguoi dung chi truyen phan can tuy bien, phan con lai dung default (BoltDB storage, static resolver, mTLS auth mac dinh).

## 5. Resilience

- Neu Node con mat ket noi len Node cha: `forwarder` retry voi backoff, log van duoc giu nguyen trong `storage` local (khong mat), tu dong flush/gui lai khi ket noi phuc hoi.
- Node cha khong can biet truoc danh sach Node con (con tu ket noi len khi khoi dong / sau khi mat ket noi).

## 6. Testing

- **Unit test**: tung implementation cua interface (storage, forwarder, resolver) duoc test doc lap voi mock/fake, khong phu thuoc network that.
- **Integration test**: dung `bufconn` (in-memory gRPC transport) de dung thu topology 3 tang (Trung tam - Chi nhanh - Leaf) trong cung 1 process; kiem tra:
  - Log tu Leaf duoc forward len den Trung tam (qua Chi nhanh trung gian).
  - Config tu Trung tam duoc push xuong den Leaf (qua Chi nhanh trung gian).
- **Test resilience**: gia lap ngat ket noi giua cac tang, kiem tra buffer local + retry/reconnect hoat dong dung, khong mat log.

## 7. Ngoai pham vi (Out of scope)

- Service registry / dynamic discovery dia chi cha (chi dung static config trong ban thiet ke nay).
- Multi-tenancy / phan quyen chi tiet giua cac Node.
- Giao dien quan tri (UI/dashboard) - framework chi cung cap phan loi (core), khong bao gom UI.
