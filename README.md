# Async Books

基于 `asyncio`、`aiohttp`、Motor 和 MongoDB 实现的异步图书采集项目。项目从
Scrape Center 图书接口并发采集列表页和详情页，使用固定数量的 Worker 消费
`asyncio.Queue`，并通过 MongoDB 唯一索引和 upsert 保证数据幂等。

## 功能特性

- 列表页并发采集，自动提取、过滤并去重图书 ID
- 固定数量的详情 Worker，避免一次创建大量 Task
- 请求超时、有限重试和指数退避
- 区分 HTTP 状态错误、超时、连接错误和 JSON 解析错误
- MongoDB 异步写入、唯一索引和幂等更新
- 自动维护 `created_at` 和 `updated_at`
- 汇总列表、详情、保存、重试、队列和运行耗时统计
- MongoDB 客户端在程序结束时统一关闭

## 项目结构

```text
async_books/
|-- config.py          # URL、分页、并发、超时、重试、MongoDB 和日志配置
|-- crawler.py         # HTTP 请求、状态码处理、重试及页面采集
|-- storage.py         # MongoDB 连接、索引、时间迁移、upsert 和关闭连接
|-- main.py            # Session、Queue、Worker、任务调度和统计汇总
|-- requirements.txt   # Python 依赖及固定版本
|-- .env.example       # 可选环境变量模板
|-- .gitignore         # Git 忽略规则
`-- README.md
```

模块依赖保持单向：

```text
main.py -> config.py
main.py -> crawler.py -> config.py
main.py -> storage.py
```

`crawler.py` 与 `storage.py` 互不依赖，不存在循环导入。

## 运行环境

- Python 3.9 或更高版本
- MongoDB，默认监听 `127.0.0.1:27017`
- 能够访问 `https://spa5.scrape.center`

## 快速开始

### 1. 进入项目目录

```powershell
cd async_books
```

### 2. 创建并启用虚拟环境

Windows PowerShell：

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

macOS 或 Linux：

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. 安装依赖

```powershell
python -m pip install -r requirements.txt
```

### 4. 启动 MongoDB

确认 MongoDB 服务已经运行。默认配置使用：

```text
mongodb://127.0.0.1:27017
```

本地 MongoDB 使用默认地址时不需要创建 `.env` 文件。

### 5. 运行采集程序

```powershell
python main.py
```

## 可选配置

项目通过 `python-dotenv` 加载 `.env`。没有 `.env` 时使用 `config.py` 中的默认值，
因此本地 MongoDB 使用默认地址时可以直接运行。

需要修改连接地址、页数、并发量或超时时，先创建 `.env`：

```powershell
Copy-Item .env.example .env
```

macOS 或 Linux：

```bash
cp .env.example .env
```

然后编辑 `.env`。该文件已被 `.gitignore` 忽略，不应提交数据库密码。

| 配置项 | 默认值 | 说明 |
| --- | ---: | --- |
| `MONGO_URI` | `mongodb://127.0.0.1:27017` | MongoDB 连接地址 |
| `MONGO_DATABASE` | `spider_center` | 数据库名称 |
| `MONGO_COLLECTION` | `spa5_books1` | 集合名称 |
| `PAGE_SIZE` | `18` | 每个列表页的数据量 |
| `PAGE_NUMBER` | `2` | 采集列表页数量 |
| `CONCURRENCY` | `5` | 详情 Worker 数量及请求并发上限 |
| `DETAIL_QUEUE_SIZE` | `10` | 详情队列最大容量 |
| `REQUEST_TOTAL_TIMEOUT` | `20` | 单次请求总超时，单位为秒 |
| `REQUEST_CONNECT_TIMEOUT` | `5` | 连接超时，单位为秒 |
| `REQUEST_READ_TIMEOUT` | `10` | 响应读取超时，单位为秒 |
| `MAX_RETRIES` | `3` | 初次请求之外的最大重试次数 |
| `BACKOFF_BASE` | `1` | 指数退避基础等待秒数 |
| `USER_AGENT` | `async-books-crawler/2.0` | HTTP User-Agent |
| `LOG_LEVEL` | `INFO` | 日志级别 |

数值配置会在 `config.py` 加载时校验。页数、并发量和超时等配置不需要修改业务代码。

## 请求与重试策略

| 场景 | 处理方式 |
| --- | --- |
| HTTP 200 | 解析 JSON 并返回数据 |
| HTTP 429 | 按指数退避等待后重试 |
| HTTP 500-599 | 按指数退避等待后重试 |
| 其他 HTTP 400-499 | 记录失败，不重试 |
| 请求超时 | 记录超时并重试 |
| `aiohttp.ClientError` | 记录客户端错误并重试 |
| Content-Type 或 JSON 异常 | 记录解析错误并重试 |

默认最多执行 1 次初始请求和 3 次重试，退避时间依次为 1、2、4 秒。重试耗尽后
返回失败，单个请求失败不会中断其他 Worker。

## 异步调度

程序首先并发采集列表页并生成去重后的 ID 集合，然后将 ID 放入有界
`asyncio.Queue`。程序只创建与 `CONCURRENCY` 相同数量的详情 Worker，每个 Worker
持续调用 `queue.get()` 消费数据，并在 `finally` 中调用 `queue.task_done()`。
主流程使用 `queue.join()` 等待队列清空，并在结束时回收所有 Worker。

这种结构使详情请求并发量始终不超过配置上限，也不会因数据量增长而一次创建
几千或几万个 Task。

## MongoDB 存储

程序启动时会：

1. 为 `id` 创建唯一索引 `unique_book_id`。
2. 为旧文档补齐缺失的 `created_at` 和 `updated_at`。
3. 使用 `update_one(..., upsert=True)` 保存详情数据。

首次插入时同时写入 UTC `created_at` 和 `updated_at`；再次采集相同 ID 时保留
`created_at` 并更新 `updated_at`。唯一索引和 upsert 共同保证重复运行不会增加重复文档。

## 运行结果

默认采集 2 页，每页 18 条。在接口数据完整且请求、存储均成功时，预期队列和
MongoDB 集合包含 36 个唯一 ID。第二次运行后集合总数仍保持 36。

程序结束时会输出类似汇总：

```text
summary | elapsed=24.92s | index_success=2 | index_failed=0 |
detail_success=36 | detail_failed=0 | saved=36 | save_failed=0 |
request_retries=0 | retry_exhausted=0 | queued=36 | worker_errors=0
```

统计字段说明：

- `index_success` / `index_failed`：列表页成功和失败数量
- `detail_success` / `detail_failed`：详情任务最终成功和失败数量
- `saved` / `save_failed`：MongoDB 保存成功和失败数量
- `request_retries`：实际执行的额外请求次数
- `retry_exhausted`：耗尽全部重试后仍失败的请求数量
- `queued`：放入详情队列的唯一 ID 数量
- `worker_errors`：Worker 捕获的非预期异常数量

只有详情请求成功且 MongoDB 保存成功时才会增加 `detail_success`。

## 常见问题

### 无法连接 MongoDB

确认 MongoDB 服务正在运行，并检查 `MONGO_URI`。需要认证时，在本机 `.env` 中
填写带用户名和密码的 URI，不要将密码写入代码或 `.env.example`。

### 无法访问采集接口

检查网络、DNS、防火墙和代理设置。程序会对连接错误、超时、HTTP 429 和服务端
错误执行有限重试，最终失败会体现在汇总统计中。

### PowerShell 禁止启用虚拟环境

可以只对当前 PowerShell 会话调整策略：

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

然后重新执行：

```powershell
.\.venv\Scripts\Activate.ps1
```
