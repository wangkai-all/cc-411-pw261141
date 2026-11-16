# costpipe — 可追溯成本分摊数据管道（虚构数据，纯标准库）

把三类**独立更新**的输入加工成**可追溯、金额守恒、版本对齐**的成本分摊结果。
不含任何付款或财务审批逻辑——这是一个数据加工与版本对齐管道。

## 三个独立进程

| 进程 | 启动 | 职责 |
|---|---|---|
| 接收服务 `ingest.py` | `python3 -m costpipe.ingest` | 唯一写方面孔：HTTP API + 网页控制台，写原始输入、发布事件、任务入队 |
| 分摊工作进程 `worker.py` | `python3 -m costpipe.worker` | 轮询任务队列，按（账期 × 情景）重算，写不可变版本 |
| 报告订阅端 `subscriber.py` | `python3 -m costpipe.subscriber` | 只走 HTTP 消费事件流，本地物化、持久化 offset、自检守恒；离线后按 seq 追平 |

三者通过 SQLite（WAL 模式）+ 仅追加事件流解耦，互不直接调用。

## 快速开始

```bash
cd costpipe
python3 -m costpipe.seed --reset          # 写入虚构组织/费用/用量/规则
python3 -m costpipe.ingest &              # http://127.0.0.1:8791/
python3 -m costpipe.worker &              # 处理队列
python3 -m costpipe.subscriber &          # 报告消费端

# 一键端到端演示（自动启停全部进程，含订阅端离线→追平）
python3 run_demo.py

# 测试
python3 tests/test_pipeline.py -v
```

> 注：处理本仓库自有代码用普通 `python3` 即可；`python3 -I`（隔离模式）
> 不会把当前目录加入 `sys.path`，因此不能配合 `-m costpipe.*` 使用。

## 核心设计如何对应需求

### 1. 三源独立更新，加工成可追溯分摊
- `charge`（费用明细）、`usage_record`（资源用量）、`org_unit`（组织关系）分表，
  各自有独立的写入 API，任何一源变更都会为相关（账期 × 情景）入队重算。
- 每条 `alloc_line` 都带 `charge_id`、`org_unit_id`、`org_path`、`weight`、规则说明，
  网页“逐笔追溯”或 `GET /api/trace/{charge_id}` 可查一笔钱完整去向。

### 2. 关系追溯更正：不能只改汇总名称
- 组织用 **SCD2**：更正 = 关闭旧版本行（写 `valid_to`）+ 插入新版本行，
  旧行及其产生的全部 `alloc_line` 永不修改、永不删除。
- 更正会对**所有历史账期**入队重算，产出新的不可变 `alloc_version`，
  事件里带 `diff_from_previous`（每个组织 old/new/delta），旧贡献被重新派生而非改名。

### 3. 两种保存策略的成本，都满足守恒与尾差可解释
- **按明细保存分摊**：每个不可变版本的全部 `alloc_line` 落库，可逐笔审计。
- **按账期重算**：`input_fingerprint`（对全部输入做规范化哈希）未变时自动跳过重算，
  避免无意义的版本膨胀；`POST /api/recompute {"force": true}` 可强制重算。
- **金额守恒**：整数分（cents）入账，最大余数法（Hamilton）分配，
  引擎内 `assert sum(分摊) == sum(账单)`；订阅端对每个版本独立复核。
- **尾差可解释**：整除产生的每 1 分都记录在 `rounding_audit` 并写到受益行的 `note`，
  不是藏在汇总里的黑箱差额。

### 4. 真实费用 vs 情景分摊
- `scenario` 表用 `is_real` 区分；真实情景固定为 `actual`，
  情景（如 `budget2027`）规则独立、版本独立，永远不会并入真实费用合计。
- 网页 KPI 与 API 都把两者分列。

### 5. 订阅端离线后追上新版本
- `publication_log` 仅追加，每个事件有单调 `seq`；订阅端在自己的本地 SQLite
  持久化 `last_seq`，离线期间不丢事件，恢复后从断点顺序回放——
  包括被 `superseded` 的旧版本，因此能看到完整更正历史而不仅是最新值。
- `run_demo.py` 第 5 步真实地杀掉订阅端进程、制造变更、再重启验证追平。

## 数据模型（见 `costpipe/db.py`）

```
charge / usage_record        原始输入（按 charge_id/usage_id 幂等去重）
org_unit                     SCD2 组织版本（valid_from/valid_to 时间窗）
rule                         版本化规则（usage | equal | fixed）
scenario                     actual(真实) / 自定义情景
job                          分摊任务队列（queued→running→done/failed）
alloc_version                不可变结果版本（supersedes 链 + 输入指纹）
alloc_line                   逐笔分摊行（含尾差标记与规则说明）
publication_log              仅追加事件流（订阅端按 seq 消费）
consumer_offset              订阅端 offset（服务端视角）
```

## 主要 HTTP API

| 方法 | 路径 | 作用 |
|---|---|---|
| POST | `/api/charges` | 补录费用（金额可传元或整数分） |
| POST | `/api/usage` | 补录资源用量 |
| POST | `/api/org/correction` | 组织 SCD2 更正并追溯重算 |
| POST | `/api/rules` | 新增规则版本（不覆盖旧版） |
| POST | `/api/scenarios` | 创建 what-if 情景 |
| POST | `/api/recompute` | 按账期重算（可 `force`） |
| GET | `/api/trace/{charge_id}` | 一笔费用的逐笔去向 |
| GET | `/api/allocations/versions/{id}/lines` | 某版本全部明细 |
| GET | `/api/events?after=N` | 事件流（订阅端轮询） |
| POST | `/api/consumers/{id}/ack` | 订阅端确认 offset |

## 边界与取舍
- 单文件 SQLite + WAL 足以演示多进程协作与版本对齐，未引入外部消息队列；
  事件流即 publication_log，订阅端 offset 即消费位点。
- 分摊落在**叶子组织**；组织合并改变的是汇报层级，若计量表仍归属原单位，
  该单位仍是叶子并继续承接费用（`run_demo.py` 第 3 步有说明）。
- 金额用整数分避免浮点误差；元金额经 `Decimal` 四舍五入转分。
