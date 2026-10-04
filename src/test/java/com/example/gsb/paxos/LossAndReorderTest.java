package com.example.gsb.paxos;

import static com.example.gsb.paxos.TestSupport.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** 需求 6：丢包与乱序——消息可延迟或被丢弃，最终仍要达成决议或明确未达成。 */
class LossAndReorderTest {

    private final ExecutorService executor = Executors.newCachedThreadPool();

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void decisionStillReachedDespiteLossAndReorder() throws Exception {
        PaxosCluster cluster = new PaxosCluster(3);
        cluster.network().setQueueingEnabled(true);
        Proposer proposer = cluster.newProposer("p1");

        Future<ProposalResult> future = executor.submit(() -> proposer.propose("X"));

        // prepare 阶段：乱序投递（排队本身即模拟了延迟）
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Prepare) == 3);
        cluster.network().deliverAllShuffled(42L);

        // promise 回复：丢掉 1 条，剩余 2 条仍构成多数派
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Promise) == 3);
        cluster.network().dropNext();
        cluster.network().deliverAllShuffled(7L);

        // accept 阶段：乱序并丢掉 1 条 Accept，2 个接受者接受即多数派
        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Accept) == 3);
        cluster.network().shuffle(13L);
        cluster.network().dropNext();
        cluster.network().deliverAll();

        await(() -> cluster.network().pendingMatching(m -> m instanceof Message.Accepted) == 2);
        cluster.network().deliverAll();

        ProposalResult result = future.get();
        assertThat(result.status()).isEqualTo(ProposalResult.Status.DECIDED);
        assertThat(result.value()).isEqualTo("X");
        assertThat(cluster.acceptorCountWithValue("X")).isEqualTo(2);
        assertThat(cluster.majorityAcceptedValue()).isEqualTo("X");
        assertThat(cluster.stats().decidedValue()).isEqualTo("X");
    }

    @Test
    void staleMessagesFromOlderBallotAreIgnored() throws Exception {
        PaxosCluster cluster = new PaxosCluster(3);
        cluster.network().setQueueingEnabled(true);
        Proposer proposer = cluster.newProposer("p1");
        proposer.setPhaseTimeout(Duration.ofMillis(150));

        Future<ProposalResult> future = executor.submit(() -> proposer.propose("X"));

        // 第 1 轮的 Prepare 被“延迟”在发件箱里，提案者超时后进入第 2 轮
        Ballot roundOne = new Ballot(1, "p1");
        Ballot roundTwo = new Ballot(2, "p1");
        await(() -> cluster.network().pendingMatching(
                m -> m instanceof Message.Prepare && m.ballot().equals(roundTwo)) == 3);

        // 只投递第 2 轮的消息，让决议在第 2 轮达成
        cluster.network().deliverMatching(m -> m.ballot().equals(roundTwo));
        await(() -> cluster.network().pendingMatching(
                m -> m instanceof Message.Promise && m.ballot().equals(roundTwo)) == 3);
        cluster.network().deliverMatching(m -> m.ballot().equals(roundTwo));
        await(() -> cluster.network().pendingMatching(
                m -> m instanceof Message.Accept && m.ballot().equals(roundTwo)) == 3);
        cluster.network().deliverMatching(m -> m.ballot().equals(roundTwo));
        await(() -> cluster.network().pendingMatching(
                m -> m instanceof Message.Accepted && m.ballot().equals(roundTwo)) == 3);
        cluster.network().deliverMatching(m -> m.ballot().equals(roundTwo));

        ProposalResult result = future.get();
        assertThat(result.status()).isEqualTo(ProposalResult.Status.DECIDED);
        assertThat(result.value()).isEqualTo("X");

        // 迟到的第 1 轮 Prepare 现在才送达：编号过低被拒绝，不破坏已达成的决议
        cluster.network().deliverMatching(m -> m.ballot().equals(roundOne));
        for (Acceptor acceptor : cluster.acceptors()) {
            assertThat(acceptor.promisedBallot()).isEqualTo(roundTwo);
            assertThat(acceptor.acceptedValue()).isEqualTo("X");
        }
        assertThat(cluster.stats().decidedValue()).isEqualTo("X");
    }

    @Test
    void totalMessageLossReportsNotDecided() {
        PaxosCluster cluster = new PaxosCluster(3);
        cluster.network().setQueueingEnabled(true);
        Proposer proposer = cluster.newProposer("p1");
        proposer.setPhaseTimeout(Duration.ofMillis(100));
        proposer.setMaxRounds(3);

        // 所有消息都滞留在发件箱中（相当于全部丢失），提案者必须明确报告未达成
        ProposalResult result = proposer.propose("X");

        assertThat(result.status()).isEqualTo(ProposalResult.Status.NOT_DECIDED);
        assertThat(cluster.stats().isDecided()).isFalse();
        assertThat(cluster.stats().decidedValue()).isNull();
        assertThat(cluster.stats().proposalRounds()).isEqualTo(3);
        assertThat(cluster.majorityAcceptedValue()).isNull();
    }
}
