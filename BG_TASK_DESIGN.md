# GPU Task Spooler — 多用户优先级调度增强设计文档

## 概述

在原有 Task Spooler (`ts`) 基础上，新增以下核心能力：

- **优先级调度**: 任务支持 0–100 优先级，高优先级优先执行
- **多用户 Fair 调度**: 同优先级任务按用户轮询 (round-robin)，防止单用户垄断
- **背景任务**: 最低优先级持久任务，用户任务到达时自动抢占，用户任务结束后自动恢复
- **Cooldown 窗口**: 用户任务提交后，N 秒内不调度背景任务（可配置，默认 120s）
- **配置文件**: 通过 `TS_BACKGROUND_CONF` 支持多个背景任务

---

## 一、架构概览

```
┌──────────────────────────────────────────────────────────┐
│  CLI: ts [--priority N] [--background] [--cooldown S]   │
│       TS_USER=alice ts -P 80 -G 2 python train.py       │
└──────────┬───────────────────────────────────────────────┘
           │ Unix Domain Socket (AF_UNIX)
           ▼
┌──────────────────────────────────────────────────────────┐
│  Server (单线程 select 事件循环)                          │
│  ┌─────────┐  ┌──────────────────┐  ┌────────────────┐  │
│  │ 消息分发 │  │  next_run_job()  │  │  preempt_bg()  │  │
│  │ 30+ Msg │  │  优先级+Fair调度  │  │  kill(-pid,    │  │
│  │  Types  │  │  同优先级轮询用户  │  │  SIGTERM)     │  │
│  └─────────┘  └──────────────────┘  └────────────────┘  │
│                     │                                     │
│         ┌───────────┴───────────┐                        │
│         ▼                       ▼                        │
│  firstjob (活跃队列)    first_finished_job (完成队列)     │
│    · QUEUED              · FINISHED                      │
│    · ALLOCATING(GPU)     · SKIPPED                       │
│    · RUNNING                                             │
│    · PREEMPTED (新)                                      │
└──────────────────────────────────────────────────────────┘
```

### 任务状态机

```
HOLDING_CLIENT → QUEUED → RUNNING → FINISHED/SKIPPED  (普通任务)
              ↘ ALLOCATING(GPU) ↗

背景任务:
  QUEUED → RUNNING → (被抢占) → QUEUED  (循环)
```

---

## 二、新增数据结构

### `struct Job` 新增字段

```c
char *user;         // 用户标识 (TS_USER 环境变量, fallback uid)
int priority;       // 优先级 0-100, 0=背景, 50=默认
int is_background;  // 是否为背景任务
```

### `struct CommandLine` 新增字段

```c
char *user;
int priority;
int is_background;
```

### `enum Jobstate` 新增

```c
PREEMPTED  // 背景任务被抢占, 等待重新调度
```

### `enum MsgTypes` 新增

```c
SET_COOLDOWN, GET_COOLDOWN, GET_COOLDOWN_OK
```

---

## 三、核心算法

### 3.1 `next_run_job()` — 优先级 + Fair 调度

```
next_run_job():
    ┌─ free_slots <= 0 → return -1
    ├─ 遍历 firstjob, 收集满足条件的候选任务:
    │   · state == QUEUED 或 ALLOCATING
    │   · 依赖已满足
    │   · slot 足够
    │   · GPU 可用 (如果需要)
    │   · 背景任务: cooldown 窗口内跳过
    ├─ 按 priority 降序排序
    ├─ 过滤最高优先级组 (max_priority)
    ├─ Fair 轮询: 选择 last_scheduled_user 之后的下一个用户
    │   若所有用户都无任务 → 选第一个
    ├─ 为选中任务分配 GPU / 更新 busy_slots
    └─ 更新 last_scheduled_user, 返回 jobid
```

**示例**: 用户 A(P=50)×2, B(P=50)×2, 1 slot → 执行顺序: A1→B1→A2→B2

### 3.2 `preempt_background_jobs()` — 背景任务抢占

```c
void preempt_background_jobs() {
    for each job in firstjob:
        if job.is_background && job.state == RUNNING:
            kill(-job.pid, SIGTERM);  // Server 直接杀进程组
}
```

触发时机: 任何 `priority > 0` 的任务提交时 (`s_newjob()` 中调用)。

### 3.3 `job_finished()` — 背景任务重入队

```c
void job_finished(const Result *result, int jobid) {
    // ... GPU 释放 ...

    if (p->is_background) {
        busy_slots -= p->num_slots;
        p->state = QUEUED;  // 重回队列等待调度
        // ... 保存结果信息 ...
        return;  // 不加入 finished 链表
    }

    // ... 原有普通任务完成逻辑 ...
}
```

### 3.4 Cooldown 窗口

- 全局变量 `cooldown_seconds` (默认 120，可通过 `--cooldown` 配置)
- `last_user_submit_time`: 每次 `priority > 0` 的任务提交时更新
- 调度时: 背景任务若 `(now - last_user_submit_time) < cooldown_seconds` 则跳过

---

## 四、配置与使用

### 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `TS_USER` | 用户标识 (用于 Fair 调度) | 空 (显示为 `-`) |
| `TS_BACKGROUND_CMD` | 单条后台命令 (Server 启动时自动提交) | 空 |
| `TS_BACKGROUND_CONF` | 后台任务配置文件路径 | 空 |

### 配置文件格式 (`TS_BACKGROUND_CONF`)

```
# 注释行以 # 开头
# 每行一个后台命令
python bg_main.py --gpu 0
python bg_monitor.py --interval 10
```

优先级: `TS_BACKGROUND_CONF` > `TS_BACKGROUND_CMD`, 二者都为空时不启动后台任务。

### 命令行选项

| 选项 | 说明 |
|------|------|
| `-P <n>` / `--priority <n>` | 设置任务优先级 (0-100, 默认 50) |
| `--background` | 标记为背景任务 (自动 priority=0, 可抢占, 持久运行) |
| `--cooldown <seconds>` | 设置冷却窗口 (默认 120) |
| `--get_cooldown` | 查询当前冷却窗口 |

### 使用示例

```bash
# 设置用户标识
export TS_USER=alice

# 普通任务 (默认 priority=50)
ts -G 1 python train.py

# 高优先级任务
ts -P 80 -G 2 python important.py

# 提交背景任务
ts --background python bg_cleanup.py

# 配置文件方式启动 (Server 自动启动后台任务)
TS_BACKGROUND_CONF=/path/to/bg.conf ts -S 2

# 设置冷却窗口为 30 秒
ts --cooldown 30

# 多用户 Fair 调度
TS_USER=alice ts -P 50 python a.py &
TS_USER=bob   ts -P 50 python b.py &  # 将与 alice 交替执行
```

---

## 五、修改文件清单

| 文件 | 行数变化 | 核心改动 |
|------|----------|----------|
| `main.h` | +15 | 数据结构、枚举、函数声明 |
| `main.c` | +20 | CLI 选项、TS_USER 读取 |
| `client.c` | +35 | NEWJOB 新字段、背景持久循环、cooldown 命令 |
| `jobs.c` | +100/-50 | 重写 `next_run_job()`、新增 `preempt_background_jobs()`、修改 `job_finished()`、cooldown 管理 |
| `server.c` | +25 | cooldown 消息分发、`fork_background_client()`、`TS_BACKGROUND_CONF` 解析 |
| `list.c` | +40 | 列表显示 P 列和 User 列 |

---

## 六、测试覆盖

测试脚本: `test_new_features.sh`，共 41 个测试用例。

### 测试分类

| 类别 | 用例数 | 覆盖内容 |
|------|--------|----------|
| A. 优先级调度 | 4 | 高优先先跑、同优先 FIFO、默认值、边界(0 vs 100) |
| B. 背景任务 | 6 | 抢占、重入队、崩溃恢复、配置文件、CMD/CONF 优先级 |
| C. Fair 调度 | 3 | 2 用户交替、混合优先级、防饥饿 |
| D. Cooldown | 3 | 设置/查询、窗口内抑制、窗口后恢复 |
| E. 并发多用户 | 2 | 同时提交、爆发提交公平性 |
| F. 边界处理 | 5 | 无 TS_USER、多 slot 混跑、优先级边界、默认值 |

### 运行方式

```bash
cd ~/task-spooler
make cpu
chmod +x test_new_features.sh
./test_new_features.sh
```

### 测试结果

```
RESULTS: 41 passed, 0 failed
All tests passed!
```

---

## 七、设计决策记录

1. **Server 直接 kill 而非消息**: 背景任务抢占采用 Server 侧 `kill(-pid, SIGTERM)` 而非发送 `PREEMPT` 消息给 Client。因为 Client 在执行任务期间阻塞在 `wait()`，无法接收消息。

2. **背景任务始终 `-f` 模式**: 后台 Client 使用 `-f` (foreground) 标志，不 fork 到后台，保持连接并持久循环等待 `RUNJOB`。

3. **Cooldown 仅抑制背景**: 冷却窗口只跳过背景任务 (`is_background && priority==0`)，不影响同优先级的用户任务。

4. **`TS_BACKGROUND_CONF` 优先级 > `TS_BACKGROUND_CMD`**: 当两者同时设置时，`TS_BACKGROUND_CONF` 优先，`TS_BACKGROUND_CMD` 被忽略。

5. **`fork_background_client` 使用 `/proc/self/exe`**: 解决 PATH 依赖问题，确保 Server 子进程能正确找到 `ts` 二进制路径。

6. **匿名用户退化为 FIFO**: 未设置 `TS_USER` 的任务 `user == NULL`，不参与 round-robin 调度（`last_scheduled_user` 不更新），退化为提交顺序 FIFO。仅设置了 `TS_USER` 的用户间才按 round-robin 调度。

7. **协议版本未变, 但 `struct Msg` 尺寸增大**: 新增 `priority`、`is_background`、`user_size` 三个 int 字段 (+12 字节)。`PROTOCOL_VERSION`(730) 未更新，新旧二进制的 Msg 结构不兼容。每次部署需确保 Client 和 Server 来自同一构建。

