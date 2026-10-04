package com.example.gsb.paxos;

import static com.example.gsb.paxos.TestSupport.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 需求 7：在两个提案者正面竞争的脚本化场景下校验统计：
 * 提案轮次、prepare/accept 消息数、被拒绝次数与达成的值。
 *
 * <p>竞争过程：p1 的 (1,p1) 与 p2 的 (1,p2) 互相抢先承诺，双方第一轮的 Accept
 * 都被拒绝；抬号后 p1 第二轮成功决议 "A"，p2 第二轮沿用 "A"。
 */
class StatsContentionTest {

    private final ExecutorService executor = Executors.newFixedThreadPool(2);

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    private static boolean prepare(Message m) {
        return m instanceof Message.Prepare;
    }

    private static boolean promise(Message m) {
        return m instanceof Message.Promise;
    }

    private static boolean accept(Message m) {
        return m instanceof Message.Accept;
    }

    private static boolean accepted(Message m) {
        return m instanceof Message.Accepted;
    }

    private static boolean reject(Message m) {
        return m instanceof Message.Reject;
    }

    @Test
    void statsReflectContentionRoundsAndRejections() throws Exception {
        PaxosCluster cluster = new PaxosCluster(3);
        cluster.network().setQueueingEnabled(true);
        Proposer p1 = cluster.newProposer("p1");
        Proposer p2 = cluster.newProposer("p2");
        p1.setPhaseTimeout(Duration.ofSeconds(2));
        p2.setPhaseTimeout(Duration.ofSeconds(2));

        Future<ProposalResult> f1 = executor.submit(() -> p1.propose("A"));
        await(() -> cluster.network().pendingMatching(StatsContentionTest::prepare) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(StatsContentionTest::promise) == 3);

        // p2 在 p1 的 Promise 尚未投递时加入竞争
        Future<ProposalResult> f2 = executor.submit(() -> p2.propose("B"));
        await(() -> cluster.network().pendingMatching(StatsContentionTest::prepare) == 3);
        cluster.network().deliverMatching(StatsContentionTest::prepare);
        await(() -> cluster.network().pendingMatching(StatsContentionTest::promise) == 6);

        // 先把 p1 的 Promise 交给 p1：p1 发出 (1,p1)="A" 的 Accept
        cluster.network().deliverMatching(
                m -> promise(m) && m.to().equals("p1"));
        await(() -> cluster.network().pendingMatching(StatsContentionTest::accept) == 3);
        // 接受者已承诺更高的 (1,p2)，p1 的 Accept 全部被拒
        cluster.network().deliverMatching(StatsContentionTest::accept);
        await(() -> cluster.network().pendingMatching(StatsContentionTest::reject) == 3);

        // 投递 p2 的 Promise 与 p1 的拒绝：p2 发 Accept，p1 抬号重发 Prepare
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(StatsContentionTest::accept) == 3
                && cluster.network().pendingMatching(StatsContentionTest::prepare) == 3);

        // p1 的 (2,p1) Prepare 先送达，接受者转而承诺 (2,p1)
        cluster.network().deliverMatching(StatsContentionTest::prepare);
        await(() -> cluster.network().pendingMatching(StatsContentionTest::promise) == 3);
        // p2 的 (1,p2) Accept 因此全部被拒
        cluster.network().deliverMatching(StatsContentionTest::accept);
        await(() -> cluster.network().pendingMatching(StatsContentionTest::reject) == 3);

        // p1 收到承诺发出 (2,p1)="A" 的 Accept；p2 抬号到第 3 轮
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(StatsContentionTest::accept) == 3
                && cluster.network().pendingMatching(StatsContentionTest::prepare) == 3);

        // p1 的 Accept 被接受，决议 "A"
        cluster.network().deliverMatching(StatsContentionTest::accept);
        await(() -> cluster.network().pendingMatching(StatsContentionTest::accepted) == 3);
        cluster.network().deliverMatching(StatsContentionTest::accepted);

        ProposalResult r1 = f1.get(5, TimeUnit.SECONDS);
        assertThat(r1.status()).isEqualTo(ProposalResult.Status.DECIDED);
        assertThat(r1.value()).isEqualTo("A");

        // p2 第 3 轮的 Promise 带有已接受的 "A"，必须沿用之
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(StatsContentionTest::promise) == 3);
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(StatsContentionTest::accept) == 3);
        assertThat(cluster.network().pendingMessages())
                .allSatisfy(m -> assertThat(((Message.Accept) m).value()).isEqualTo("A"));
        cluster.network().deliverAll();
        await(() -> cluster.network().pendingMatching(StatsContentionTest::accepted) == 3);
        cluster.network().deliverAll();

        ProposalResult r2 = f2.get(5, TimeUnit.SECONDS);
        assertThat(r2.status()).isEqualTo(ProposalResult.Status.DECIDED);
        assertThat(r2.value()).isEqualTo("A");

        PaxosStats stats = cluster.stats();
        // p1 两轮 + p2 两轮（第一轮被拒，抬号后第三轮编号），共 4 轮
        assertThat(stats.proposalRounds()).isEqualTo(4);
        // 4 轮 × 3 个接受者
        assertThat(stats.prepareMessages()).isEqualTo(12);
        assertThat(stats.promiseMessages()).isEqualTo(12);
        assertThat(stats.preparePhaseMessages()).isEqualTo(24);
        // p1 两轮 Accept + p2 两轮 Accept
        assertThat(stats.acceptMessages()).isEqualTo(12);
        // 只有最后两轮成功（p1 第 2 轮 + p2 第 3 轮）
        assertThat(stats.acceptedMessages()).isEqualTo(6);
        assertThat(stats.acceptPhaseMessages()).isEqualTo(18);
        // 双方第一轮各 3 条 Accept 被拒
        assertThat(stats.rejections()).isEqualTo(6);
        assertThat(stats.decidedValue()).isEqualTo("A");
        assertThat(cluster.majorityAcceptedValue()).isEqualTo("A");
    }
}
