# FileCabinet 文件柜

以 SHA-256 为 key 的文件存储服务。Go + Redis 协调 ModelScope；React 前端构建后嵌入单个 Go 二进制。

## 已实现

- 浏览器在 Web Worker 中每次读取 4 MiB，增量计算 SHA-256，不将大文件整体载入内存。
- 默认 `UPLOAD_MODE=proxy`。≤5 MiB 经后端校验并 base64 内联提交；更大文件由后端携带认证流式转发至精确校验的 ModelScope LFS 接口（或不带认证转发至真正的存储签名地址），校验实际大小与 SHA-256 后才提交。流量经过后端，内存不随大文件大小增长。上传连接失败不自动重放流。
- `direct` 提供申请、直传、完成接口，浏览器可实验性尝试大文件直传，未来 CLI / 原生客户端也可复用；仅在上游返回真正的免 token 签名 URL 或 blob 已存在时可用。当前 ModelScope LFS 返回需认证的 API URL，申请新 blob 直传会明确拒绝（422），应使用 `proxy`。原生客户端不受 CORS 限制不等于免认证；浏览器需要上游允许 CORS（或仅在测试时使用 Allow CORS 插件），插件不解决鉴权，小文件仍走内联接口。**不包含 CLI，不下发 ModelScope token，不提供“不安全 302”开关。**
- 文件默认永久；1h / 1d / 7d / 自定义绝对到期时间。重复内容只延长有效期，永久优先。proxy 重复上传仍读取并校验传入内容，避免声明 hash 绕过内容校验。
- 六位数字短码；本次首次数字冲突后改试六位大小写字母与数字混合，Redis SET NX 原子占位。默认到期 `min(文件到期, 当前时间+7天)`；自定义必须未来且严格早于默认值。文件到期后不能生成/使用短码。
- 下载查询返回本服务相对链接，后端携带凭证请求 ModelScope，跟随受限存储重定向时不携带凭证。文件不会在页面公开列出。
- 元数据无 TTL，Redis sorted set 保留清理任务；短码和直传会话才使用 TTL。每 30 秒清理最多 100 项，明确拒绝的失败后 60 秒重试；超时/5xx 等未知结果隔离待人工核对；同一文件的上传/续期/删除/下载在锁内串行。

## 先决条件与启动

Go ≥1.24、Bun 1.x、Redis 7，以及已存在、有写权限的 ModelScope 仓库。仓库/指定 namespace 由本服务独占管理，不能同时用 SDK 或其他程序修改。使用公共 npm registry，仓库不含私有 registry 锁文件。

目录：

```text
rizxfrog/
├── FileCabinet/       # Go 后端
└── FileCabinetFront/  # React 前端，包含源码、bun.lock
```

```sh
cd FileCabinet
cp .env.example .env
# 编辑 .env，填写 MODELSCOPE_TOKEN 与 MODELSCOPE_REPO_ID；不要提交 .env
# 启动本地 Redis（需要 AOF/noeviction），或使用下面 Docker Compose

go run .
# 另一个终端：
cd ../FileCabinetFront
bun install --frozen-lockfile
bun run dev
# 打开 http://127.0.0.1:5173，Vite 同源代理 /api 到 8080
```

后端初次运行前 `public/` 已包含生成 UI；修改前端后运行：

```sh
cd FileCabinet
./scripts/build.sh
./bin/filecabinet
```

脚本会替换生成目录 `public/`，源码不受影响。可设置 `FRONTEND_DIR` 覆盖兄弟目录。部署时只需二进制、`.env` 和 Redis；静态资源已嵌入，不依赖工作目录中的 public。

### Docker

```sh
cp .env.example .env  # 填写凭证后再运行
# 从 FileCabinet 目录执行，构建上下文是两个仓库的共同父目录
# Dockerfile.dockerignore 排除两边 .env、.git、node_modules 等
docker compose up --build -d
```

Compose 的 Redis 开启 AOF、`appendfsync always`、`noeviction` 并持久化 volume；Redis 不映射到宿主端口。应用只映射宿主 127.0.0.1:8080；**不要扩容副本**。停止宽限 31 分钟匹配默认传输上限 30 分钟；调大 `TRANSFER_TIMEOUT` 时也要相应调大宽限。

### 配置

见 `.env.example`，已有进程环境变量优先于 `.env`。

| 配置 | 默认/说明 |
|---|---|
| `LISTEN_ADDR` | `127.0.0.1:8080` |
| `REDIS_URL` | 标准 `redis://` / `rediss://` DSN |
| `MODELSCOPE_ENDPOINT` | `https://www.modelscope.cn`，必须 HTTPS origin |
| `MODELSCOPE_TOKEN` | 必填，永不写入日志/API/下载链接 |
| `MODELSCOPE_REPO_ID` | 必填，`owner/name` |
| `MODELSCOPE_REPO_TYPE` | `datasets` 或 `models` |
| `MODELSCOPE_REVISION` | `master`；**dataset 仅支持 master**，因为 SDK 的删除端点没有 Revision 参数；其他值启动报错。model 上传/下载/删除一致使用配置 revision |
| `FILECABINET_NAMESPACE` | 两位数字，默认 `00` |
| `UNSAFE_DIRECT_EXPOSE_URL` | 默认 `false`。仅用于 direct/CORS 实验；开启后会把上游上传 href 返回浏览器，但不会返回 token。此开关危险，测试后必须立即关闭；proxy 模式忽略。 |
| `UPLOAD_MODE` | `proxy`（默认）/ `direct` |
| `MAX_UPLOAD_BYTES` | 10 GiB，可调整；最大 2^50 字节 |
| `TRANSFER_TIMEOUT` | `30m`，1 秒到 24 小时 |
| `STORAGE_HOST_SUFFIXES` | `.modelscope.cn,.aliyuncs.com,.amazonaws.com`；仅接受这些边界明确的 HTTPS 主机、443 端口、公共 IP；管理员可添加经过验证的域名。不要放宽为 `.com` |

存储路径：`namespace/hash[:2]/hash[2:]`。Redis key 前缀是 endpoint/repo type/repo/revision/namespace 的摘要，避免不同存储混用。

## API

时间全部为 Unix 秒，`expires_at: 0` 表示永久文件/短码使用默认到期时间。hash 为 64 位小写 SHA-256。错误统一为 `{"error":"可读说明"}`，400 参数错误、404 不存在/过期、409 不确定操作待处理、502 上游错误、503 服务不可用、504 超时。

### 服务状态

- `GET /healthz`：验证 Redis 中本实例仍持有 owner claim。
- `GET /api/config`：公开 `upload_mode`、`max_upload_bytes`、`inline_limit`，不包含任何凭证。

### 浏览器/代理上传

```http
PUT /api/files/{hash}?size=5&name=hello.txt&expires_at=0
Content-Type: application/octet-stream
Content-Length: 5

hello
```

成功 201 返回 `{hash,name,size,expires_at,created_at}`。body 为原始文件而不是 multipart；`size` 必填。内容/大小不符不提交；无内容文件也可以归档。名称最多 255 UTF-8 字节，不允许路径分隔符或 NUL/CR/LF。完成提交和元数据保存后，文件的最终合并到期时间必须仍晚于当前时间，否则返回过期错误；已提交的过期文件仍保留元数据和清理索引供清理，不误报成功。

需要短码时上传成功后调用 `POST /api/codes`。短码上限依据已保存文件的合并有效期，而不是本次请求的较短有效期。前端将“文件成功、短码失败”分别展示，不误报文件失败；取消短码阶段只中止等待，文件保留，服务端可能已经生成短码。

### 实验性浏览器 / 原生客户端直传（仅 direct）

```http
POST /api/uploads/prepare
Content-Type: application/json

{"hash":"<sha256>","size":6000000,"name":"archive.bin","expires_at":0}
```

**能力限制：** 参考 SDK 与保存的实测 batch 响应，当前地址形如 `https://lfs.modelscope.cn/api/v1/repos/{models|datasets}/{owner}/{repo}/blobs/{sha256}/{size}`，没有自包含签名，需要 Bearer + `m_session_id` Cookie。这类目标返回 422 与切换 `proxy` 的提示，不创建上传会话、不下发 token。CLI 原生 HTTP 只能避开 CORS，不能补足认证。

仅当返回真正的无额外认证要求、带查询参数的存储签名 URL，或 blob 已存在时，返回 `{session_id,upload_url,method:"PUT",expires_in:3600}`。仅支持 >5 MiB。会话有效期 1 小时；上游 URL 本身可能更早到期，URL 过期可重新申请。

1. `upload_url` 非空：客户端对其 PUT 原始文件，原生客户端正确设置 Content-Length（浏览器自动设置），**不要添加 ModelScope token**。
2. URL 为空：上游 blob 已存在，可以跳过 PUT。
3. `POST /api/uploads/{session_id}/complete`：后端再次调用 batch API，只有确认该 blob 不再需要上传才提交并保存记录。

浏览器在 hash 计算后执行上述三步，支持上传进度与各阶段取消；PUT 失败不会请求 complete。取消 complete 等待并不能撤回服务端提交，请通过 hash 查询最终结果；未完成的直传会话在 1 小时后过期。外部上传不携带 Cookie 或 Authorization；仅接受原始签名地址的成功响应，重定向响应不触发 complete。direct 模式的 CSP connect-src 只额外允许 `STORAGE_HOST_SUFFIXES` 中的 HTTPS 主域及子域，proxy 模式仍仅允许同源。浏览器可能在被允许的存储域间跟随重定向，因此这些域必须可信；不要扩大白名单来绕过未知错误。

direct 不读取文件内容，依赖上游按 oid/size 验证 blob；不像 proxy 自行验证字节。会话 ID 是短时持有者凭证，不能公开。客户端声称完成不作为成功证据。不要在浏览器中尝试用重定向绕过 PUT CORS。

### 查询与下载

```http
GET /api/files/{hash或短码}
```

返回 `{file:{hash,name,size,expires_at,created_at},download_url:"/api/download/<key>"}`。链接由本后端代理、直接下载，不是 token URL。短码查询返回的文件信息含 hash，**因此短码不是对已知 hash 的撤权机制**。

```http
GET /api/download/{hash或短码}
```

attachment 下载；当前不支持 Range/断点续传。下载持有文件锁到流关闭，在已开始的下载结束后才能清理。链接请求开始时到期就拒绝；已开始传输不会在到期秒数到达时强行中断。文件名安全编码，不回传上游任意响应头。

### 生成短码

```http
POST /api/codes
Content-Type: application/json

{"hash":"<sha256>","expires_at":0}
```

201 返回 `{code,hash,expires_at}`。数字首次冲突后最多重试 31 次混合短码，再返回失败；`SET NX` 保证冲突不覆盖。大写与小写不同。

## 单实例与故障恢复（必读）

ModelScope mutation API 没有 fencing token，Redis 有 TTL 的锁不能阻止网络延迟中的旧删除。MVP **只支持每个存储身份一个活跃后端**：

- 启动以随机 token `SET NX` 获得**无 TTL** 的 `fc:<identity>:owner`，失败不开启 HTTP。
- 进程内按 hash 的 256 个条带锁协调操作（不同 hash 可能保守地串行）；Redis 保存 `pending` hash journal，在提交/删除前记录，成功持久化本地结果才移除。
- 提交返回错误/超时，其远端结果可能未知：文件保持 pending，拒绝续期、下载或重试提交，不报告成功。
- 明确拒绝（例如 403/429）的删除失败保留 metadata/index/journal，可后台重试；超时、5xx、响应损坏等不确定结果记为 `delete-uncertain`，绝不自动重试后解锁，以免旧删除迟到破坏新上传。所有未确认删除都拒绝续期。
- 停机先停止接入并等待 handlers 与清理退出。只有无 pending、无不确定本地 I/O，才原子比较 owner token 后释放。任何在途操作/未确定结果都不因超时“假定取消成功”。
- 崩溃、SIGKILL、断电或未确定结果会保留 owner；**这时重启被阻止是保护机制，不是自动恢复 bug**。不支持 HA、自动接管、滚动多副本；可用性让位于一致性。
- Redis 数据丢失、回滚、切换到缺少最新记录的副本会破坏该保护。必须使用持久化/noeviction，备份元数据；不能 FLUSHDB 或删除 volume 来“修复”。即使 AOF 也不能宣称远端操作有数学 fencing 保证。

### 人工恢复步骤

仅在管理员确认旧进程完全停止、没有另一实例/外部写入者、远端在途操作已完成并核对后执行：

1. 停止所有 FileCabinet 进程与入口写入；保存 Redis 和仓库状态备份。查看启动日志中的 `storage_identity`，并核对 repo/revision/namespace，避免操作其他存储前缀。
2. 使用 Redis 管理工具读取该前缀的 `owner`、`pending`、相关 `file:<hash>` 与 `expires`。不要把凭证或会话 URL 放到公开工单。
3. 按 pending 中的 `commit`/`delete`/`delete-inflight`/`delete-uncertain` 对照 ModelScope 当前 revision 的路径。远端超时请求可能仍执行，**不能仅凭客户端已退出就认定已取消**；等待/向上游确认到稳定终态。
4. 对已完成 commit：人工补齐正确 metadata/到期索引（依据原请求，未知有效期不能猜），或在确认后从远端删除孤立路径。对 delete：确认路径删除，再移除该文件 metadata/index。仍存在的过期文件可保留 metadata/index，待重启后重试。核对完成后移除对应 pending entry。metadata 原样保留直至清理/恢复完成，不能盲删。
5. 只有 `pending` 为空、远端结果全部已确定、无旧进程时，才由管理员**比较刚核对的 owner 值并删除 owner key**，随后启动单个实例。不要设置自动 TTL，不要无条件 DEL owner。没有提供一键解锁脚本，避免跳过前置条件。

服务运行时 Redis 操作失败则当前业务 fail closed。异常详情不返回给浏览器（避免泄漏 DSN 或签名 URL）。

## 安全与存储限制

- 本 MVP **没有账户、权限、访问限速或容量配额**。持有 hash/短码者可以获取文件；hash 不等于登录权限，六位码可枚举。默认只绑定 loopback；上公网前必须通过 TLS 反向代理增加认证、IP/请求速率限制、请求体限制和上传配额。不要公开托管敏感文件。写入接口检查浏览器 Origin，但这不是身份鉴权。
- 前端开发走 Vite 同源代理；生产反代应保留 Host 和正确的 `X-Forwarded-Proto`。对外代理需清洗客户端伪造的 forwarding 头；后端不应直接暴露。
- 每实例最多同时处理 16 个 HTTP 请求，包含传输；大文件整体超时默认 30 分钟，小文件 base64 内联会短时占用多份 ≤5 MiB 缓冲。proxy 模式不省带宽，下载同样中转；没有自动全文件重试或断点上传。
- 存储转发地址只来自经过认证的 ModelScope 响应，不接受客户端传入目标 URL。存储连接二次解析/固定公共 IP，禁用环境 HTTP 代理与自动重定向，凭证仅发给配置的 ModelScope API origin，以及精确匹配 `https://lfs.modelscope.cn` + 配置仓库类型/ID + 预期 SHA-256/大小的标准上传路径。LFS 上传 URL 必须无查询、转义变体或片段，仓库/oid/size 任一不匹配即拒绝；其他 `.modelscope.cn` 上传路径也拒绝。存储域名后缀白名单绝不授权附加 token，OSS 与所有下载重定向均不带认证。白名单外地址、非 HTTPS、未知上传 headers/parameters 或非零 offset 返回错误，不静默降级。已知 batch 的 `upload_header: {"Range":"bytes=0-"}` 是完整上传提示，SDK 同样不依赖传递该 header。
- 文件过期会尽快从当前仓库树删除；**Git 历史提交、LFS 全局 blob、存储侧缓存可能仍保留内容**。删除不等于不可恢复的物理擦除。文件 hash 或持有的上游签名 URL 不支持撤销；严格数据销毁需求不适合此后端。
- 停机期间过期任务保留，恢复后执行；删除失败文件立即逻辑不可访问，物理删除会延迟。已发出的字节/链接无法收回。
- 不自动创建仓库，不扫描重建 Redis；Redis 备份不可省略。ModelScope 对私有仓库的具体权限、API 变动、域名及服务配额需实际环境联调。

## 验证

```sh
go test ./... -count=1
go test -race ./...
go vet ./...
cd ../FileCabinetFront
bun test
bun run build
# 可选浏览器回归（需 Chromium，或设置下列本机 Chrome 路径）：
PLAYWRIGHT_CHROMIUM_EXECUTABLE=/usr/bin/google-chrome bun run test:e2e
```

后端包含 miniredis + 模拟 ModelScope 的上传/查询/下载/短码 HTTP smoke，expiry、重复上传、字节校验、失败不登记、清理重试/并发、owner 防重复与停机 drain、direct 确认、SDK wire 格式/凭证隔离测试。回归覆盖真实记录形状的 LFS batch、精确认证范围、直传能力拒绝、mutation 缺失/冲突成功字段时保留 journal，以及 commit/save 跨过文件到期时间。commit/delete 要求显式 `Code:200`（也接受字符串 `"200"`），`Success` 若存在必须为布尔 true；`{}`、`null` 等不算确认成功。真正存储 PUT 的空 2xx 响应与 ModelScope API envelope 分开处理。浏览器测试在模拟 API 下验证真实 Web Worker hash、UI 流程与 CSP，另覆盖短码阶段取消、重复永久文件申请短期上传但生成更长短码、桌面/移动端 select 内容高度；**不代表真实 ModelScope 上传/私有下载/删除已验证**。没有使用真实 token 或执行远端破坏性测试。
