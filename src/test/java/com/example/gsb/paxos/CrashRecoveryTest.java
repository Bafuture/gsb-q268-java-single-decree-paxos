package com.example.gsb.paxos;

import static com.example.gsb.paxos.TestSupport.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** 需求 5：提案者崩溃与恢复——恢复后重新提案不得违反已达成的一致性。 */
class CrashRecoveryTest {

    private final ExecutorService executor = Executors.newCachedThreadPool();

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    /** 让 {@code proposer} 的提案推进到“多数派已接受、Accepted 尚未投递”的状态。 */
    private void driveAcceptPhaseToCompletion(PaxosCluster cluster) {
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Prepare) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Promise) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Accept) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Accepted) == 3);
    }

    private void driveToCompletion(PaxosCluster cluster) {
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Prepare) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Promise) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Accept) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Accepted) == 3);
        cluster.network().deliverAll();
    }

    @Test
    void recoveredProposerMustNotViolateEstablishedConsensus() throws Exception {
        PaxosCluster cluster = new PaxosCluster(3);
        cluster.network().setQueueingEnabled(true);
        Proposer proposer = cluster.newProposer("p1");

        // 第一次提案 "X"：多数派已接受，但提案者在得知结果前崩溃
        Future<ProposalResult> first = executor.submit(() -> proposer.propose("X"));
        driveAcceptPhaseToCompletion(cluster);
        proposer.crash();
        cluster.network().dropAll(); // Accepted 确认全部丢失
        assertThat(first.get()).extracting(ProposalResult::status)
                .isEqualTo(ProposalResult.Status.CRASHED);
        assertThat(cluster.acceptorCountWithValue("X")).isEqualTo(3);

        // 恢复后改提 "Y"：必须沿用已被多数派接受的 "X"
        proposer.recover();
        Future<ProposalResult> second = executor.submit(() -> proposer.propose("Y"));
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Prepare) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Promise) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Accept) == 3);
        assertThat(cluster.network().pendingMessages())
                .allSatisfy(m -> assertThat(((Message.Accept) m).value()).isEqualTo("X"));
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Accepted) == 3);
        cluster.network().deliverAll();

        ProposalResult result = second.get();
        assertThat(result.status()).isEqualTo(ProposalResult.Status.DECIDED);
        assertThat(result.value()).isEqualTo("X");
        assertThat(cluster.acceptorCountWithValue("Y")).isZero();
        assertThat(cluster.majorityAcceptedValue()).isEqualTo("X");
        assertThat(cluster.stats().decidedValue()).isEqualTo("X");
    }

    @Test
    void crashedProposerAbortsPromptlyAndIgnoresLateMessages() throws Exception {
        PaxosCluster cluster = new PaxosCluster(3);
        cluster.network().setQueueingEnabled(true);
        Proposer proposer = cluster.newProposer("p1");

        Future<ProposalResult> future = executor.submit(() -> proposer.propose("X"));
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Prepare) == 3);
        proposer.crash();
        // 崩溃后到达的消息被忽略，propose 尽快以 CRASHED 返回
        cluster.network().deliverAll();
        assertThat(future.get()).extracting(ProposalResult::status)
                .isEqualTo(ProposalResult.Status.CRASHED);
        assertThat(cluster.stats().isDecided()).isFalse();
    }

    @Test
    void crashBeforeAnyAcceptDoesNotBlockDifferentValue() throws Exception {
        PaxosCluster cluster = new PaxosCluster(3);
        cluster.network().setQueueingEnabled(true);
        Proposer proposerA = cluster.newProposer("pA");

        // pA 只完成 prepare 阶段就崩溃，尚未有任何值被接受
        Future<ProposalResult> first = executor.submit(() -> proposerA.propose("X"));
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Prepare) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Promise) == 3);
        proposerA.crash();
        cluster.network().dropAll();
        assertThat(first.get()).extracting(ProposalResult::status)
                .isEqualTo(ProposalResult.Status.CRASHED);

        // pB 用更高编号提案 "Y"：没有任何旧值被接受，可以决议自己的值
        Proposer proposerB = cluster.newProposer("pB");
        Future<ProposalResult> second = executor.submit(() -> proposerB.propose("Y"));
        driveToCompletion(cluster);

        ProposalResult result = second.get();
        assertThat(result.status()).isEqualTo(ProposalResult.Status.DECIDED);
        assertThat(result.value()).isEqualTo("Y");
        assertThat(cluster.stats().decidedValue()).isEqualTo("Y");
    }
}
