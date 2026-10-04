package com.example.paxos;

import com.example.paxos.Message.AcceptRequest;
import com.example.paxos.Message.Nack;
import com.example.paxos.Message.Prepare;
import com.example.paxos.Message.Promise;
import com.example.paxos.Proposer.Outcome;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class PaxosTest {

    private static ProposalNumber n(long round, String proposer) {
        return new ProposalNumber(round, proposer);
    }

    // ---------- 1) 两阶段流程 + 7) 统计 ----------

    @Test
    @DisplayName("两阶段流程：prepare 获多数承诺后 accept 获多数接受，决议完成")
    void twoPhaseFlowDecidesValue() {
        Cluster cluster = Cluster.withAcceptors(3);
        Proposer proposer = cluster.newProposer("P1");

        Outcome outcome = proposer.propose("X", 3);

        assertThat(outcome.decided()).isTrue();
        assertThat(outcome.value()).isEqualTo("X");
        // 多数派（至少 2 个）接受者已接受该值
        long acceptedCount = cluster.acceptors().stream()
                .filter(a -> "X".equals(a.acceptedValue()))
                .count();
        assertThat(acceptedCount).isGreaterThanOrEqualTo(2);

        // 统计：一轮、3 条 prepare、3 条 accept、0 次拒绝、达成值 X
        PaxosStats stats = proposer.stats();
        assertThat(stats.getRounds()).isEqualTo(1);
        assertThat(stats.getPrepareMessages()).isEqualTo(3);
        assertThat(stats.getAcceptMessages()).isEqualTo(3);
        assertThat(stats.getRejections()).isZero();
        assertThat(stats.getDecidedValue()).isEqualTo("X");
    }

    @Test
    @DisplayName("两阶段流程：5 个接受者时多数派为 3")
    void twoPhaseFlowWithFiveAcceptors() {
        Cluster cluster = Cluster.withAcceptors(5);
        Proposer proposer = cluster.newProposer("P1");

        Outcome outcome = proposer.propose("hello", 2);

        assertThat(outcome.decided()).isTrue();
        assertThat(outcome.value()).isEqualTo("hello");
        long acceptedCount = cluster.acceptors().stream()
                .filter(a -> "hello".equals(a.acceptedValue()))
                .count();
        assertThat(acceptedCount).isGreaterThanOrEqualTo(3);
    }

    // ---------- 2) 承诺规则 ----------

    @Test
    @DisplayName("承诺规则：承诺高编号后拒绝更低编号的 prepare 与 accept")
    void promiseRuleRejectsLowerNumbers() {
        Acceptor acceptor = new Acceptor("A1");

        // 收到编号 5 的 prepare：承诺
        assertThat(acceptor.handle(new Prepare(n(5, "P1"), "P1", "A1")))
                .isInstanceOf(Promise.class);
        assertThat(acceptor.promised()).isEqualTo(n(5, "P1"));

        // 更低编号的 prepare 被拒绝
        assertThat(acceptor.handle(new Prepare(n(3, "P2"), "P2", "A1")))
                .isInstanceOf(Nack.class);
        // 更低编号的 accept 同样被拒绝
        assertThat(acceptor.handle(new AcceptRequest(n(4, "P2"), "v", "P2", "A1")))
                .isInstanceOf(Nack.class);
        // 承诺编号不被低编号消息拉低
        assertThat(acceptor.promised()).isEqualTo(n(5, "P1"));
        assertThat(acceptor.acceptedValue()).isNull();

        // 更高编号的 prepare 重新获得承诺
        assertThat(acceptor.handle(new Prepare(n(6, "P2"), "P2", "A1")))
                .isInstanceOf(Promise.class);
        assertThat(acceptor.promised()).isEqualTo(n(6, "P2"));
    }

    @Test
    @DisplayName("承诺规则：Promise 会携带已接受的旧值供后续提案者沿用")
    void promiseCarriesAcceptedValue() {
        Acceptor acceptor = new Acceptor("A1");
        acceptor.handle(new Prepare(n(1, "P1"), "P1", "A1"));
        acceptor.handle(new AcceptRequest(n(1, "P1"), "old", "P1", "A1"));

        Message response = acceptor.handle(new Prepare(n(2, "P2"), "P2", "A1"));

        assertThat(response).isInstanceOf(Promise.class);
        Promise promise = (Promise) response;
        assertThat(promise.acceptedNumber()).isEqualTo(n(1, "P1"));
        assertThat(promise.acceptedValue()).isEqualTo("old");
    }

    // ---------- 3) 旧值沿用（安全性） ----------

    @Test
    @DisplayName("旧值沿用：已接受的旧值必须被后续提案者沿用，不得覆盖")
    void proposerMustAdoptAlreadyAcceptedValue() {
        Cluster cluster = Cluster.withAcceptors(3);
        Proposer p1 = cluster.newProposer("P1");
        Proposer p2 = cluster.newProposer("P2");

        // P1 完成阶段一，并让部分接受者接受值 "X"（随后崩溃，没来得及完成多数派）
        assertThat(p1.preparePhase()).isTrue();
        cluster.acceptor("A1").handle(new AcceptRequest(p1.currentNumber(), "X", "P1", "A1"));
        cluster.acceptor("A2").handle(new AcceptRequest(p1.currentNumber(), "X", "P1", "A2"));
        p1.crash();

        // P2 用更高编号提案自己的值 "Y"
        assertThat(p2.preparePhase()).isTrue();
        // 安全性关键断言：P2 必须放弃 "Y"，沿用编号最高的已接受值 "X"
        assertThat(p2.chooseValue("Y")).isEqualTo("X");

        Optional<String> decided = p2.acceptPhase("Y");
        assertThat(decided).contains("X");
        // 最终多数派接受的是旧值 "X"，没有任何接受者接受 "Y"
        assertThat(cluster.acceptors().stream()
                .map(Acceptor::acceptedValue)
                .filter(Objects::nonNull))
                .containsOnly("X");
    }

    @Test
    @DisplayName("旧值沿用：存在多个旧值时选择编号最高的那个")
    void proposerAdoptsValueWithHighestAcceptedNumber() {
        Cluster cluster = Cluster.withAcceptors(3);
        // A1 在编号 5 接受过 "v5"，A2 在编号 9 接受过 "v9"，A3 未接受过
        cluster.acceptor("A1").handle(new Prepare(n(5, "P0"), "P0", "A1"));
        cluster.acceptor("A1").handle(new AcceptRequest(n(5, "P0"), "v5", "P0", "A1"));
        cluster.acceptor("A2").handle(new Prepare(n(9, "P0"), "P0", "A2"));
        cluster.acceptor("A2").handle(new AcceptRequest(n(9, "P0"), "v9", "P0", "A2"));

        Proposer proposer = cluster.newProposer("P1");
        // 第 1 轮编号太低被拒，Nack 帮助提案者追赶编号；第 2 轮获得承诺
        assertThat(proposer.preparePhase()).isFalse();
        assertThat(proposer.preparePhase()).isTrue();

        assertThat(proposer.chooseValue("own")).isEqualTo("v9");
    }

    // ---------- 4) 并发提案者竞争 ----------

    @Test
    @DisplayName("并发竞争：交错执行两阶段，最终只有一个值被多数派接受")
    void concurrentProposersConvergeToSingleValue() {
        Cluster cluster = Cluster.withAcceptors(3);
        Proposer p1 = cluster.newProposer("P1");
        Proposer p2 = cluster.newProposer("P2");

        // 经典交错：P1 拿到多数承诺后，P2 用更高编号抢走承诺
        assertThat(p1.preparePhase()).isTrue();
        assertThat(p2.preparePhase()).isTrue();
        // P1 的 accept 编号已过时，被多数派拒绝
        assertThat(p1.acceptPhase("X")).isEmpty();
        // P2 的 accept 成功，决议 "Y"
        assertThat(p2.acceptPhase("Y")).contains("Y");

        // P1 崩溃后重试自己的值，也必须收敛到已决议的 "Y"
        p1.crash();
        Outcome retry = p1.propose("X", 3);
        assertThat(retry.decided()).isTrue();
        assertThat(retry.value()).isEqualTo("Y");

        // 整个集群中被多数派接受的值唯一
        Set<String> acceptedValues = new HashSet<>();
        for (Acceptor acceptor : cluster.acceptors()) {
            if (acceptor.acceptedValue() != null) {
                acceptedValues.add(acceptor.acceptedValue());
            }
        }
        assertThat(acceptedValues).containsExactly("Y");
        assertThat(p1.stats().getRejections()).isGreaterThan(0);
    }

    @Test
    @DisplayName("并发竞争：两个提案者同时完整提案，决议值唯一")
    void simultaneousProposersDecideAtMostOneValue() {
        Cluster cluster = Cluster.withAcceptors(5);
        Proposer p1 = cluster.newProposer("P1");
        Proposer p2 = cluster.newProposer("P2");

        // 交替推进阶段，模拟并发竞争
        boolean p1Prepared = p1.preparePhase();
        boolean p2Prepared = p2.preparePhase();
        Optional<String> r1 = p1Prepared ? p1.acceptPhase("alpha") : Optional.empty();
        Optional<String> r2 = p2Prepared ? p2.acceptPhase("beta") : Optional.empty();

        // 编号更高的 P2 胜出；P1 重试后必须收敛到同一个值
        assertThat(r2).contains("beta");
        assertThat(r1).isEmpty();
        Outcome retry = p1.propose("alpha", 3);
        assertThat(retry.decided()).isTrue();
        assertThat(retry.value()).isEqualTo("beta");
    }

    // ---------- 5) 崩溃与恢复 ----------

    @Test
    @DisplayName("崩溃恢复：决议达成后提案者崩溃，恢复或其他提案者都不得推翻决议")
    void crashAfterDecidePreservesConsistency() {
        Cluster cluster = Cluster.withAcceptors(3);
        Proposer p1 = cluster.newProposer("P1");

        // P1 让多数派接受 "X" 后立刻崩溃（没来得及记住决议结果）
        assertThat(p1.preparePhase()).isTrue();
        assertThat(p1.acceptPhase("X")).contains("X");
        p1.crash();

        // P1 恢复后重新提案，仍得到 "X"
        Outcome recovered = p1.propose("X", 3);
        assertThat(recovered.decided()).isTrue();
        assertThat(recovered.value()).isEqualTo("X");

        // 另一个提案者提出不同的值，也必须沿用已达成的 "X"
        Proposer p2 = cluster.newProposer("P2");
        Outcome other = p2.propose("Y", 3);
        assertThat(other.decided()).isTrue();
        assertThat(other.value()).isEqualTo("X");
    }

    @Test
    @DisplayName("崩溃恢复：阶段一中途崩溃，恢复后用更高编号重新提案")
    void crashMidProtocolThenRecover() {
        Cluster cluster = Cluster.withAcceptors(3);
        Proposer p1 = cluster.newProposer("P1");

        // P1 完成阶段一后在发 accept 前崩溃，易失状态全部丢失
        assertThat(p1.preparePhase()).isTrue();
        long roundBeforeCrash = p1.currentNumber().round();
        p1.crash();
        assertThat(p1.currentNumber()).isNull();

        // 恢复后重新提案：编号更高，且能正常达成决议
        Outcome outcome = p1.propose("recovered", 3);
        assertThat(outcome.decided()).isTrue();
        assertThat(outcome.value()).isEqualTo("recovered");
        assertThat(p1.currentNumber().round()).isGreaterThan(roundBeforeCrash);
        assertThat(p1.stats().getRounds()).isGreaterThanOrEqualTo(2);
    }

    // ---------- 6) 丢包与乱序 ----------

    @Test
    @DisplayName("丢包：少数派消息丢失仍能达成决议")
    void minorityPacketLossStillDecides() {
        Cluster cluster = Cluster.withAcceptors(3);
        Proposer proposer = cluster.newProposer("P1");
        // A3 与外界的通信全部丢失
        cluster.network().setDropRule(m -> m.to().equals("A3") || m.from().equals("A3"));

        Outcome outcome = proposer.propose("V", 3);

        assertThat(outcome.decided()).isTrue();
        assertThat(outcome.value()).isEqualTo("V");
    }

    @Test
    @DisplayName("丢包：多数派不可达时明确报告未达成")
    void majorityPacketLossReportsUndecided() {
        Cluster cluster = Cluster.withAcceptors(3);
        Proposer proposer = cluster.newProposer("P1");
        // A2、A3 全部失联，永远凑不齐多数派
        cluster.network().setDropRule(m ->
                m.to().equals("A2") || m.to().equals("A3")
                        || m.from().equals("A2") || m.from().equals("A3"));

        Outcome outcome = proposer.propose("V", 2);

        assertThat(outcome.decided()).isFalse();
        assertThat(outcome.value()).isNull();
        assertThat(proposer.stats().getDecidedValue()).isNull();
        assertThat(proposer.stats().getRounds()).isEqualTo(2);
    }

    @Test
    @DisplayName("丢包：首轮 prepare 全部丢失，下一轮重试后达成决议")
    void transientLossRetriesWithNewRound() {
        Cluster cluster = Cluster.withAcceptors(3);
        Proposer proposer = cluster.newProposer("P1");
        // 只丢前 3 条 prepare（第一轮），之后恢复
        int[] dropped = {0};
        cluster.network().setDropRule(m -> {
            if (m instanceof Prepare && dropped[0] < 3) {
                dropped[0]++;
                return true;
            }
            return false;
        });

        Outcome outcome = proposer.propose("V", 5);

        assertThat(outcome.decided()).isTrue();
        assertThat(outcome.value()).isEqualTo("V");
        // 统计可见：用了 2 轮，prepare 消息 6 条（含丢失的 3 条）
        assertThat(proposer.stats().getRounds()).isEqualTo(2);
        assertThat(proposer.stats().getPrepareMessages()).isEqualTo(6);
        assertThat(proposer.stats().getAcceptMessages()).isEqualTo(3);
    }

    @Test
    @DisplayName("乱序：开启随机乱序投递后仍能达成决议（多种子）")
    void reorderedDeliveryStillDecides() {
        for (long seed = 0; seed < 10; seed++) {
            Cluster cluster = Cluster.withAcceptors(3);
            Proposer p1 = cluster.newProposer("P1");
            Proposer p2 = cluster.newProposer("P2");
            cluster.network().enableReordering(new Random(seed));

            // 两个提案者竞争，乱序下最终只能决议出一个值
            Outcome o1 = p1.propose("one", 5);
            Outcome o2 = p2.propose("two", 5);

            assertThat(o1.decided() || o2.decided()).isTrue();
            Set<String> decidedValues = new HashSet<>();
            if (o1.decided()) {
                decidedValues.add(o1.value());
            }
            if (o2.decided()) {
                decidedValues.add(o2.value());
            }
            // 若双方都宣布决议，值必须相同
            assertThat(decidedValues).hasSize(1);
        }
    }

    @Test
    @DisplayName("乱序：迟到的低编号 prepare 到达后被拒绝，不破坏已达成决议")
    void staleDelayedPrepareIsRejected() {
        Cluster cluster = Cluster.withAcceptors(3);
        Proposer p1 = cluster.newProposer("P1");
        Proposer p2 = cluster.newProposer("P2");

        // P1 的 prepare 被延迟在网络中
        cluster.network().setDelayRule(m -> m instanceof Prepare prepare && prepare.from().equals("P1"));
        assertThat(p1.preparePhase()).isFalse();
        cluster.network().setDelayRule(m -> false);
        assertThat(cluster.network().delayedCount()).isEqualTo(3);

        // P2 正常完成两阶段，决议 "Y"
        Outcome outcome = p2.propose("Y", 3);
        assertThat(outcome.decided()).isTrue();
        assertThat(outcome.value()).isEqualTo("Y");

        // 迟到的旧 prepare 乱序到达：全部因承诺规则被拒，已接受值不变
        cluster.network().releaseDelayed(true, new Random(1));
        cluster.network().deliverAll();
        for (Acceptor acceptor : cluster.acceptors()) {
            assertThat(acceptor.promised()).isEqualTo(n(1, "P2"));
            assertThat(acceptor.acceptedValue()).isEqualTo("Y");
        }
    }
}
