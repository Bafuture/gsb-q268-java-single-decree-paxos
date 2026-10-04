# 单决议 Paxos 组件

Pair-wise GSB 标注任务仓库（第 17 批 / 268）。

| 项目 | 内容 |
|------|------|
| 任务类型 | Feature 迭代 |
| 任务难度 | 困难 |
| 语言/框架 | Java, Maven, JUnit 5 |
| 环境可复现等级 | 无外部依赖 |
| 构建方式 | Maven（含 mvnw wrapper，无需本机安装 Maven） |

> 本仓库是**初始环境快照**：只有工程骨架，不含任何实现代码。
> 分支说明：`main` 为初始环境；`A`、`B` 为两次独立执行各自的工作分支，均从 `main` 的同一个提交拉出。

## 运行方式

```bash
./mvnw -q verify
```

## 实现说明

代码位于 `src/main/java/com/example/gsb/paxos/`：

| 类 | 职责 |
|----|------|
| `Ballot` | 提案编号 `(round, proposerId)`，全序比较，保证不同提案者编号互不冲突 |
| `Message` | 协议消息：`Prepare`/`Promise`/`Accept`/`Accepted`/`Reject` |
| `Acceptor` | 接受者状态机：承诺编号、已接受编号与值 |
| `Proposer` | 提案者：prepare/accept 两阶段、旧值沿用、被拒抬号重试、崩溃/恢复 |
| `InMemoryNetwork` | 内存网络：立即或排队投递，支持丢弃、乱序、延迟（排队即延迟） |
| `PaxosStats` | 统计：提案轮次、两阶段消息数、被拒绝次数、达成的值 |
| `PaxosCluster` | 集群装配与多数派校验辅助 |

### 两阶段流程

1. **Prepare**：提案者用递增编号广播 `Prepare`，收齐多数派 `Promise` 才进入下一阶段；
2. **Accept**：向全体接受者广播 `Accept(ballot, value)`，收齐多数派 `Accepted` 即达成决议。

### 承诺规则及其作用

接受者一旦对编号 `b` 做出承诺，就拒绝一切编号小于 `b` 的 `Prepare`/`Accept`。
该规则是 Paxos 安全性的基石：若某个值已被多数派接受，则任何更小编号的提案都
无法再让多数派接受别的值；而任何更大编号的提案在 prepare 阶段必然与“接受过该值的
多数派”相交，从而打听到旧值并沿用之。两条规则共同保证集群不会先后决议出两个
不同的值。

### 提案值选择

提案者从多数派 `Promise` 中收集各接受者汇报的 `(acceptedBallot, acceptedValue)`，
若存在已接受的旧值，必须沿用其中 `acceptedBallot` 最大的那个值，绝不用自己的值覆盖；
只有多数派都没有接受过任何值时，才允许提议自己的值。

### 崩溃与恢复

`crash()` 立即中止当前提案并丢弃后续消息；对象内持久化的 `nextRound` 在
`recover()` 后继续递增，恢复后的提案编号严格更大，只能重新走两阶段并沿用旧值，
不会违反已达成的一致性。

### 丢包与乱序

`InMemoryNetwork` 排队模式下，测试可显式 `dropNext()`（丢包）、`shuffle(seed)`
（乱序）、延迟投递；提案者按超时重试新轮次，最终要么达成决议，要么在耗尽
`maxRounds` 后明确返回 `NOT_DECIDED`。

### 测试

`src/test/java/com/example/gsb/paxos/` 共 17 个用例，覆盖：两阶段流程
（`TwoPhaseFlowTest`）、承诺规则（`AcceptorPromiseRuleTest`）、旧值沿用安全性
（`ValueAdoptionTest`）、并发竞争（`ConcurrentProposersTest`）、崩溃恢复
（`CrashRecoveryTest`）、丢包乱序（`LossAndReorderTest`）与竞争场景统计
（`StatsContentionTest`）。

## 任务提示词

以下为本题完整的 User Prompt 原文，两次执行必须使用完全相同的文本。

我们要让一组节点对某一个值达成一致，且要能容忍提案者中途崩溃。请从零实现一个单决议 Paxos 组件。仓库目前只有一个空的 Maven 工程（pom.xml 只声明 JUnit 5 与 AssertJ），节点与消息用内存对象模拟。要求：1) 支持 prepare 与 accept 两阶段：提案者先收集承诺，再请求接受，获得多数派后决议完成；2) 承诺规则：接受者收到更高编号的 prepare 时必须承诺不再接受更低编号的提案，需说明该规则的作用；3) 提案值选择：提案者收到已接受的旧值时必须沿用编号最高的那个值，不得覆盖，需有测试证明安全性；4) 支持并发提案者竞争：多个提案者同时提案时最终只能有一个值被多数派接受；5) 支持提案者崩溃与恢复：崩溃后重新提案不得违反已达成的一致性；6) 支持丢包与乱序：消息可延迟或被丢弃，最终仍要能达成决议或明确未达成；7) 提供统计：提案轮次、prepare 与 accept 消息数、被拒绝次数与达成的值；8) 测试覆盖两阶段流程、承诺规则、旧值沿用、并发竞争、崩溃恢复与丢包乱序；`mvn -q verify` 一条命令跑通。

## 提交要求

1. 在本仓库中完成提示词要求的全部内容。
2. `./mvnw -q verify` 必须通过。
3. 完成后在所属分支（A 或 B）上提交，产物快照的父提交必须是初始环境快照。
