# GPU Task Spooler (ts) 用户手册

## 一、简介

Task Spooler (`ts`) 是一个 Unix 任务队列系统，帮助你管理 CPU/GPU 任务的排队与调度。类似 SLURM，但面向单机环境。

### 核心概念

- **Server（服务器）**: 后台守护进程，管理任务队列和调度。首次运行 `ts` 时自动启动，无需手动干预
- **Client（客户端）**: 每次执行 `ts` 命令的进程，与 Server 通过 Unix Socket 通信
- **Slot（槽位）**: 并发控制单元，Server 可同时运行的任务数上限（`-S` 设置，默认 1）
- **Job（任务）**: 你提交的待执行命令

---

## 二、背景任务（Background Task）

### 2.1 行为概述

背景任务是持续运行的后台作业。它一直在执行（或排队等待执行），直到被用户任务抢占。

```
正常状态:
  bg 执行 → 结束 → 自动重新排队 → 再次执行 → 循环

用户任务到来:
  bg 正在执行 → 被 SIGTERM 杀死 → 重新排队
  用户任务开始执行
  用户任务结束 → bg 自动恢复执行（冷却延迟后）
```

### 2.2 优先级与调度

| 类型 | 优先级 | 行为 |
|------|--------|------|
| 用户任务 | 1-100（默认 50） | 正常调度，完成后标记为 `finished` |
| 背景任务 | 0（固定） | 持久循环，完成后回到 `queued` 状态 |

调度规则：
1. **优先级优先**: 所有排队任务中，优先级（Priority）高的先执行
2. **同优先级 Fair**: 同优先级的多个用户按轮询（Round-Robin）交替执行，防止单一用户垄断
3. **背景最低优先**: 只有没有用户任务排队时，背景任务才会被执行

### 2.3 抢占（Preemption）

当用户提交新任务（Priority > 0）时，Server 会立即终止正在运行的背景任务（发送 SIGTERM 到其进程组），为你的任务腾出资源。

- 抢占是**立刻的**，不需要等待背景任务自然结束
- 背景任务的执行结果（包括被 kill）会被记录，然后重新排队

### 2.4 冷却延迟（Cooldown Window）

为防止背景任务在你提交完用户任务后立即抢占回来，有一个冷却窗口（Cooldown Window）：用户任务提交后的 N 秒内，即使没有其他用户任务排队，背景任务也不会被调度。

- **默认值**: 120 秒
- **重置规则**: 每次有 Priority > 0 的新任务提交，倒计时重置
- **配置方式**: `ts --cooldown <seconds>`（设置）, `ts --get_cooldown`（查询）

```
时间线:
  t=0:    bg 正在运行
  t=1s:   用户提交任务 → bg 被杀，冷却开始（120s）
  t=3s:   用户任务完成
  t=119s: bg 仍在排队等待（冷却窗口内）
  t=121s: bg 恢复执行（冷却窗口过期）
```

### 2.5 提交背景任务

**方式一：命令行提交**（推荐用于简单场景）
```bash
ts --background sh -c 'while true; do echo "bg at $(date)" >> /tmp/bg.log; sleep 60; done'
```
该命令会持续运行，直到你通过 `ts -K` 或 `ts -r <id>` 终止。

**方式二：环境变量配置**（推荐用于生产环境）

在启动 Server 之前设置环境变量：
```bash
export TS_BACKGROUND_CMD="sh -c 'while true; do python bg_cleanup.py; sleep 300; done'"
ts    # 首次运行 ts 时会自动提交该背景任务
```

**方式三：配置文件**（推荐用于多个后台任务）
```bash
# 创建配置文件 /path/to/bg.conf
cat > /path/to/bg.conf << 'EOF'
# 每行一个后台命令
python bg_main.py
python bg_monitor.py
EOF

# 启动时指定配置文件
TS_BACKGROUND_CONF=/path/to/bg.conf ts
```

优先级: `TS_BACKGROUND_CONF` > `TS_BACKGROUND_CMD`, 两者都不设置时不启动背景任务。

---

## 三、命令行选项

### 3.1 用户身份

设置 `TS_USER` 环境变量来标识你的身份，用于多用户公平调度（Fair Scheduling）：

```bash
export TS_USER=alice
ts -P 60 python train.py
```

未设置 `TS_USER` 时，用户列显示 `-`，按提交顺序 FIFO 调度（不参与轮询）。

### 3.2 任务操作（Actions）

以下选项每次只能使用一个，它们对已有任务执行操作。

| 选项 | 说明 |
|------|------|
| `-l` | **列出任务列表**（默认操作）。显示所有活跃和已完成的任务，包括 ID、状态、输出文件、错误等级、耗时、优先级、用户、命令 |
| `-K` | **终止 Server**。关闭整个任务队列系统，不再接受新任务（正在运行的任务不受影响） |
| `-C` | **清除已完成任务列表**。移除所有 `finished` 状态的任务记录 |
| `-S [num]` | **查看/设置最大并发数**（Slots）。不带参数时查看当前值，带参数时设置新值 |
| `-t [id]` | **实时查看任务输出**。类似 `tail -n 10 -f`。不指定 id 时查看最后一个任务 |
| `-c [id]` | **查看任务完整输出**。类似 `cat`。不指定 id 时查看最后一个任务 |
| `-p [id]` | **查看任务 PID**。显示正在运行的任务进程 ID |
| `-o [id]` | **查看输出文件路径**。显示任务输出被存储到了哪个文件 |
| `-i [id]` | **查看任务详情**。显示命令、环境变量、入队/开始/结束时间等 |
| `-s [id]` | **查看任务状态**。输出状态字符串（queued / allocating / running / finished / skipped） |
| `-r [id]` | **移除任务**。从队列中删除指定任务（不能删除正在运行或队列头的任务） |
| `-w [id]` | **等待任务完成**。阻塞直到指定任务结束，返回其退出码 |
| `-k [id]` | **终止任务**。向指定任务的进程组发送 SIGTERM 信号 |
| `-T` | **终止所有运行中的任务**。向所有 `running` 状态的任务发送 SIGTERM |
| `-u [id]` | **将任务提到队列首位**。不指定 id 时对最后一个添加的任务操作 |
| `-U [id-id]` | **交换两个任务**。交换它们在队列中的位置（格式如 `0-3`） |
| `-h` | **显示帮助信息** |
| `-V` | **显示版本信息** |

### 3.3 添加任务时的选项（Options Adding Jobs）

| 选项 | 说明 |
|------|------|
| `--priority \| -P <n>` | **设置优先级**（Priority）。范围 0-100，默认 50。0 为背景任务级别 |
| `--background` | **标记为背景任务**（Background Task）。自动设 Priority=0，持久循环运行，可被抢占后自动恢复 |
| `--cooldown <seconds>` | **设置冷却窗口**（Cooldown Window）。用户任务提交后多少秒内不调度背景任务，默认 120 |
| `--get_cooldown` | **查询当前冷却窗口**值 |
| `-n` | **不存储输出**。任务的标准输出（stdout）和标准错误（stderr）不会被保存到文件 |
| `-E` | **分离 stderr**。标准错误单独保存到一个 `.e` 后缀的文件中 |
| `-O <name>` | **自定义日志文件名**。指定输出文件的名称（不含路径） |
| `-z` | **压缩输出**。将存储的输出用 gzip 压缩（需要 `-n` 以外的选项） |
| `-f` | **前台运行**。不 fork 到后台，ts 进程会阻塞直到任务完成 |
| `-m` | **邮件通知**。任务完成时将输出通过 sendmail 发送邮件 |
| `-d` | **依赖上一个任务**。等上一个提交的任务结束后才开始执行 |
| `-D <id,...>` | **依赖指定任务**。等指定 ID 的任务都结束后才开始执行 |
| `-W <id,...>` | **依赖指定任务的成功**。等指定 ID 的任务都成功（退出码 0）后才开始执行 |
| `-L <label>` | **添加标签**。为任务设置一个可读名称，在列表中显示 |
| `-N <num>` | **占用多个槽位**。任务需要的并发槽位数（默认 1）。用于需要多 CPU 核的任务 |
| `-B` | **队列满时不等待**。当 Server 连接数满时立即退出，而不是无限等待 |

### 3.4 环境变量

| 变量 | 说明 |
|------|------|
| `TS_USER` | **用户标识**，用于 Fair 调度区分不同用户 |
| `TS_BACKGROUND_CMD` | **背景任务命令**，Server 启动时自动提交 |
| `TS_BACKGROUND_CONF` | **背景任务配置文件**，每行一个命令，`#` 为注释 |
| `TS_SOCKET` | Unix Socket 路径，默认 `/tmp/socket-ts.<uid>` |
| `TS_SLOTS` | 最大并发数（Slots），Server 启动时读取 |
| `TS_MAXFINISHED` | 已完成任务保留数量的上限，默认 1000 |
| `TS_MAXCONN` | 最大客户端连接数 |
| `TS_SAVELIST` | Server 被 SIGTERM 杀死时的任务列表备份文件 |
| `TS_ONFINISH` | 任务结束时调用的外部命令（传入 jobid、退出码、输出文件、命令） |
| `TS_MAILTO` | `-m` 选项的邮件收件人 |
| `TS_ENV` | 任务入队时调用的命令，其输出决定了任务的环境信息 |
| `TMPDIR` | 输出文件和默认 Socket 的存放目录 |

### 3.5 长选项操作（Long Option Actions）

| 选项 | 说明 |
|------|------|
| `-M [format]` | **序列化任务列表**。支持的格式：`default`（默认表格）、`json`（JSON 数组）、`tab`（Tab 分隔） |
| `-a [id]` | **查看任务标签**。不指定 id 时查看最后一个任务 |
| `-F [id]` | **查看完整命令**。不指定 id 时查看最后一个任务 |
| `-R` | **统计运行中任务数** |
| `-q` | **查看最后提交的任务 ID** |
| `--getenv [var]` | 获取 Server 环境变量 |
| `--setenv [var]` | 设置 Server 环境变量 |
| `--unsetenv [var]` | 删除 Server 环境变量 |
| `--get_logdir` | 查看日志目录路径 |
| `--set_logdir [path]` | 设置日志目录路径 |

### 3.6 GPU 相关选项（仅 GPU 构建可用）

| 选项 | 说明 |
|------|------|
| `-G [num]` | 任务需要的 GPU 数量（默认 1） |
| `-g [id,...]` | 直接指定 GPU 索引（如 `-g 0,2`），跳过自动分配 |
| `-g` | 列出所有 GPU 任务及其对应的 GPU ID |
| `TS_VISIBLE_DEVICES` | 限制 ts 可见的 GPU 范围（如 `0,2,3`） |

---

## 四、使用示例

### 基础用法

```bash
# 提交一个任务
ts python train.py

# 提交并等待完成（前台模式）
ts -f python train.py

# 查看任务队列
ts -l

# 查看最后一个任务的输出
ts -c

# 实时跟踪任务输出
ts -t
```

### 优先级调度

```bash
# 低优先级任务
TS_USER=alice ts -P 30 python low_priority.py

# 高优先级任务
TS_USER=alice ts -P 80 python important.py

# ---- ts -l 输出 ----
# ID   State      Output      E-Level  Time   P    User       Command
# 0    running    ...         0        3.00s  80   alice      python important.py
# 1    queued     ...         0        0.00s  30   alice      python low_priority.py
```

### 多用户 Fair 调度

```bash
# 两个用户分别提交任务，自动交替执行
TS_USER=alice ts python a1.py
TS_USER=alice ts python a2.py
TS_USER=bob   ts python b1.py
TS_USER=bob   ts python b2.py

# 执行顺序: a1 → b1 → a2 → b2（而非 a1 → a2 → b1 → b2）
```

### 背景任务完整示例

```bash
# 1. 配置背景任务
export TS_BACKGROUND_CMD="sh -c 'while true; do date >> /tmp/bg.log; sleep 30; done'"

# 2. 设置并发数
export TS_SLOTS=2

# 3. 设置冷却窗口为 60 秒
ts --cooldown 60

# 4. 之后每次提交用户任务，背景任务都会被抢占，60s 冷却后自动恢复
TS_USER=alice ts -P 50 python train.py
```

### 检查背景任务状态

```bash
# 查看是否在运行
ts -l | grep "sh -c"

# 查看背景任务产生的日志
cat /tmp/bg.log
```

---

## 五、任务状态说明

| 状态 | 英文 | 含义 |
|------|------|------|
| queued | Queued | 排队中，等待调度 |
| allocating | Allocating | 正在分配 GPU 资源 |
| running | Running | 正在执行 |
| finished | Finished | 已完成（用户任务） |
| skipped | Skipped | 被跳过（依赖任务返回非零退出码） |

背景任务不会进入 `finished`/`skipped` 状态，它们始终在 `queued` ↔ `running` 之间循环。
