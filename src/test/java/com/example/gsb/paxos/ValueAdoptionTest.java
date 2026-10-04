package com.example.gsb.paxos;

import static com.example.gsb.paxos.TestSupport.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 需求 3：提案值选择——收到已接受的旧值时必须沿用编号最高的旧值，不得覆盖。
 * 这两个测试同时也是 Paxos 安全性的直接证明。
 */
class ValueAdoptionTest {

    private final ExecutorService executor = Executors.newCachedThreadPool();

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void mustAdoptValueAlreadyAcceptedByMajorityInsteadOfOwnValue() throws Exception {
        PaxosCluster cluster = new PaxosCluster(3);
        cluster.network().setQueueingEnabled(true);

        // pA 的提案在 accept 阶段已被全部接受者接受，但它在看到 Accepted 前崩溃
        Proposer proposerA = cluster.newProposer("pA");
        Future<ProposalResult> futureA = executor.submit(() -> proposerA.propose("OLD"));

        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Prepare) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Promise) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Accept) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Accepted) == 3);

        proposerA.crash();
        cluster.network().dropAll();
        assertThat(futureA.get()).extracting(ProposalResult::status)
                .isEqualTo(ProposalResult.Status.CRASHED);

        // "OLD" 已经在全部接受者上，pB 想提案 "NEW"，必须沿用 OLD
        Proposer proposerB = cluster.newProposer("pB");
        Future<ProposalResult> futureB = executor.submit(() -> proposerB.propose("NEW"));

        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Prepare) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Promise) == 3);
        cluster.network().deliverAll();

        // 安全性证明：pB 实际发出的 Accept 全部带着 OLD，NEW 从未出现在任何消息里
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Accept) == 3);
        List<Message> accepts = cluster.network().pendingMessages();
        assertThat(accepts).hasSize(3)
                .allSatisfy(m -> assertThat(((Message.Accept) m).value()).isEqualTo("OLD"));
        assertThat(accepts).noneSatisfy(m ->
                assertThat(((Message.Accept) m).value()).isEqualTo("NEW"));

        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Accepted) == 3);
        cluster.network().deliverAll();

        ProposalResult result = futureB.get();
        assertThat(result.status()).isEqualTo(ProposalResult.Status.DECIDED);
        assertThat(result.value()).isEqualTo("OLD");
        assertThat(cluster.acceptorCountWithValue("OLD")).isEqualTo(3);
        assertThat(cluster.acceptorCountWithValue("NEW")).isZero();
        assertThat(cluster.majorityAcceptedValue()).isEqualTo("OLD");
        assertThat(cluster.stats().decidedValue()).isEqualTo("OLD");
    }

    @Test
    void picksValueOfHighestAcceptedBallotWhenPromisesReportDifferentValues() throws Exception {
        PaxosCluster cluster = new PaxosCluster(3);
        cluster.network().setQueueingEnabled(true);

        // 1) pX 的 (1,pX)=alpha 只被 acceptor-1 接受（少数派），pX 未达成决议
        Proposer proposerX = cluster.newProposer("pX");
        proposerX.setMaxRounds(1);
        proposerX.setPhaseTimeout(Duration.ofMillis(300));
        Future<ProposalResult> futureX = executor.submit(() -> proposerX.propose("alpha"));
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Prepare) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Promise) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Accept) == 3);
        cluster.network().deliverMatching(
                m -> m instanceof Message.Accept && m.to().equals("acceptor-1"));
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Accepted) == 1);
        assertThat(futureX.get()).extracting(ProposalResult::status)
                .isEqualTo(ProposalResult.Status.NOT_DECIDED);
        cluster.network().dropAll();

        // 2) pY 的 prepare 只收到 acceptor-2/3 的承诺（看不到 alpha），于是提案自己的
        //    beta，且 (1,pY)=beta 只被 acceptor-2 接受（少数派），pY 未达成决议
        Proposer proposerY = cluster.newProposer("pY");
        proposerY.setMaxRounds(1);
        proposerY.setPhaseTimeout(Duration.ofMillis(300));
        Future<ProposalResult> futureY = executor.submit(() -> proposerY.propose("beta"));
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Prepare) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Promise) == 3);
        cluster.network().deliverMatching(
                m -> m instanceof Message.Promise && !m.from().equals("acceptor-1"));
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Accept) == 3);
        cluster.network().deliverMatching(
                m -> m instanceof Message.Accept && m.to().equals("acceptor-2"));
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Accepted) == 1);
        assertThat(futureY.get()).extracting(ProposalResult::status)
                .isEqualTo(ProposalResult.Status.NOT_DECIDED);
        cluster.network().dropAll();

        // 现状：acceptor-1 持有 (1,pX)=alpha，acceptor-2 持有 (1,pY)=beta（编号更高）
        assertThat(cluster.acceptors().get(0).acceptedValue()).isEqualTo("alpha");
        assertThat(cluster.acceptors().get(1).acceptedValue()).isEqualTo("beta");

        // 3) pB 提案 "gamma"：承诺中同时出现 alpha 与 beta，必须沿用编号最高的 beta
        Proposer proposerB = cluster.newProposer("pB");
        Future<ProposalResult> futureB = executor.submit(() -> proposerB.propose("gamma"));

        // 第一轮 (1,pB) 编号低于已承诺编号，全部被拒，pB 抬高到第 2 轮
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Prepare) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Reject) == 3);
        cluster.network().deliverAll();

        // 第二轮：只投递 acceptor-1 与 acceptor-2 的承诺（分别带 alpha 与 beta）
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Prepare) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Promise) == 3);
        cluster.network().deliverMatching(
                m -> m instanceof Message.Promise && m.from().equals("acceptor-1"));
        cluster.network().deliverMatching(
                m -> m instanceof Message.Promise && m.from().equals("acceptor-2"));
        cluster.network().dropAll();

        // 安全性证明：pB 发出的 Accept 必须携带编号最高旧值 beta，而不是自己的 gamma
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Accept) == 3);
        assertThat(cluster.network().pendingMessages())
                .allSatisfy(m -> assertThat(((Message.Accept) m).value()).isEqualTo("beta"));

        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Accepted) == 3);
        cluster.network().deliverAll();

        assertThat(futureB.get().value()).isEqualTo("beta");
        assertThat(cluster.majorityAcceptedValue()).isEqualTo("beta");
        assertThat(cluster.stats().decidedValue()).isEqualTo("beta");
        assertThat(cluster.acceptorCountWithValue("gamma")).isZero();
    }
}
