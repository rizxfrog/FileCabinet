## 目标

实现一个“key, file”架构的文件存储服务，key即文件hash，下游调用端自己计算文件hash，然后通过服务端将文件上传到modelscope中（默认浏览器上传经后端流式中转；原生客户端仅在上游返回真正免 token 签名 URL 或 blob 已存在时可选择直传）。proxy 模式下后端承担文件带宽与连接压力，但大文件采用有界内存流式转发。

## 功能：

**上传文件**：
用户上传文件，上传成功后获取文件的hash。
用户可以选择是否有过期时间，默认永不过期。有过期时间即过期时后端从modelscope中删除该文件。过期时间选项有：1h, 1d, 7d, custom
用户还可以选择是否生成 "short code" 。“short code” 是一个6位的数字或者“数字+字母”，生成策略：每次尝试生成6位数字，冲突则改为生成6位数字+字母。`“short code” 的过期时间 = min(文件过期时间, 当前时间 + 7d)`。文件永不过期时，短码默认有效期为 7 天；短码不会延长文件寿命。

**下载文件**：
用户可以输入hash或者 short code 获取文件的下载链接。

**生成short code**:
用户可以输入hash生成short code，默认过期时间=min(hash过期时间, 当前时间 + 7d)。可以自定义过期时间（需晚于当前时间，且严格小于默认过期时间）。若这个hash对应的文件不存在或已过期，则拒绝生成。

**重复上传与上传通道**：
同一个 hash 重复上传，文件过期时间取 max(当前的过期时间, 新上传设置的过期时间)，只延长、不缩短；永不过期视为无限大。过期时间比较使用绝对时间。
小文件（≤5 MiB）允许经过后端，采用 ModelScope SDK 的 base64 内联提交流程；大文件默认使用 `UPLOAD_MODE=proxy`，浏览器将文件发送到后端，由后端向精确校验的 ModelScope LFS 地址附加认证并流式转发，校验 SHA-256/大小后提交。当前 ModelScope LFS 返回的并非预签名 URL，而是需认证的 API 地址；`UPLOAD_MODE=direct` 对这类新 blob 目标返回明确不支持，仅为真正免 token 的签名存储地址或已存在 blob 保留未来原生接口；浏览器不支持大文件直传，当前不实现 CLI。ModelScope token 不下发给客户端。

## 后端代码仓库

/home/van/github/rizxfrog/FileCabinet

技术栈：Go语言 + Redis

## 前端代码仓库

/home/van/github/rizxfrog/FileCabinetFront

技术栈：Typescript + React + shadcn + TailwindCSS + bun

## 构建方式：

开发时前端 bun run dev
生成时前端 bun run build, 并将构建产物放在后端的 public 目录下，后端编译时一起打包进编译产物。

## references

modelscope的官方python sdk,以及我的代码分析md文档在： ~/github/modelscope_hub
由于这个项目是用Go语言写的，因此你需要自己实现一套Go操作modelscope的infrastructure层

## modelscope 的存储策略

仓库根目录下，首先是一个两位数字的命名空间(FileCabinet namespace)，例如 00, 05, 16 等
接下来是文件hash的前2位字符的文件夹，文件按hash放入对应的文件夹里,例如 30298d9a8dd621809fba0a8b83f1cff0d625427b71770d5a05067142fae4b9ec，文件路径就是 00/30/298d9a8dd621809fba0a8b83f1cff0d625427b71770d5a05067142fae4b9ec
```
.
└── 00
    └── 30
        └── 298d9a8dd621809fba0a8b83f1cff0d625427b71770d5a05067142fae4b9ec
```

## 配置文件

使用 .env 作为配置文件。也是为了方便docker部署。
.env 里写了 redis dsn, modelscope的login token, repo_id, repotype type(datasets | models) 等

## MVP 部署边界

每个存储身份（endpoint/repo type/repo/revision/namespace）只允许一个活跃后端，使用 Redis 无 TTL 的 owner claim 与进程内文件锁，崩溃或不确定远端结果需要人工核对恢复；不支持多副本自动接管。Redis 必须持久化且 noeviction。dataset 仅支持 master（官方 SDK 删除 API 不携带 Revision）。公开/私有下载均由后端认证代理，不暴露 token。删除作用于当前仓库树，不保证清除 Git 历史或 LFS 物理 blob。详见 README。
