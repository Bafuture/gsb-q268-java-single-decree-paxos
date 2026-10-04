package com.example.gsb.paxos;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** 需求 1：prepare/accept 两阶段流程，多数派后决议完成。 */
class TwoPhaseFlowTest {

    @Test
    void singleProposerDecidesAfterTwoPhases() {
        PaxosCluster cluster = new PaxosCluster(3);
        Proposer proposer = cluster.newProposer("p1");

        ProposalResult result = proposer.propose("hello");

        assertThat(result.status()).isEqualTo(ProposalResult.Status.DECIDED);
        assertThat(result.value()).isEqualTo("hello");

        // 三个接受者都接受了该值，达到多数派
        assertThat(cluster.acceptorCountWithValue("hello")).isEqualTo(3);
        assertThat(cluster.majorityAcceptedValue()).isEqualTo("hello");

        // 每个接受者都记录了承诺与接受状态
        for (Acceptor acceptor : cluster.acceptors()) {
            assertThat(acceptor.promisedBallot()).isEqualTo(new Ballot(1, "p1"));
            assertThat(acceptor.acceptedBallot()).isEqualTo(new Ballot(1, "p1"));
            assertThat(acceptor.acceptedValue()).isEqualTo("hello");
        }
    }

    @Test
    void statsAreCollectedForTwoPhaseRun() {
        PaxosCluster cluster = new PaxosCluster(3);
        Proposer proposer = cluster.newProposer("p1");

        proposer.propose("v");

        PaxosStats stats = cluster.stats();
        assertThat(stats.proposalRounds()).isEqualTo(1);
        // prepare 阶段：3 条 Prepare + 3 条 Promise
        assertThat(stats.prepareMessages()).isEqualTo(3);
        assertThat(stats.promiseMessages()).isEqualTo(3);
        assertThat(stats.preparePhaseMessages()).isEqualTo(6);
        // accept 阶段：3 条 Accept + 3 条 Accepted
        assertThat(stats.acceptMessages()).isEqualTo(3);
        assertThat(stats.acceptedMessages()).isEqualTo(3);
        assertThat(stats.acceptPhaseMessages()).isEqualTo(6);
        assertThat(stats.rejections()).isZero();
        assertThat(stats.isDecided()).isTrue();
        assertThat(stats.decidedValue()).isEqualTo("v");
    }

    @Test
    void decisionNeedsMajorityNotUnanimity() {
        // 5 个接受者，其中一个永远收不到消息（其消息被丢弃），3 票即可达成决议
        PaxosCluster cluster = new PaxosCluster(5);
        cluster.network().setQueueingEnabled(true);
        Proposer proposer = cluster.newProposer("p1");

        Thread thread = new Thread(() -> proposer.propose("x"));
        thread.start();
        try {
            // 丢掉发给 acceptor-5 的 Prepare，其余照常投递
            TestSupport.await(() -> cluster.network().pendingMatching(
                    m -> m instanceof Message.Prepare) == 5);
            cluster.network().dropMatching(m -> m.to().equals("acceptor-5"));
            cluster.network().deliverAll();
            TestSupport.await(() -> cluster.network().pendingMatching(
                    m -> m instanceof Message.Promise) == 4);
            cluster.network().deliverAll();
            // 丢掉发给 acceptor-5 的 Accept，其余 4 个接受者接受即超过多数派
            TestSupport.await(() -> cluster.network().pendingMatching(
                    m -> m instanceof Message.Accept) == 5);
            cluster.network().dropMatching(m -> m.to().equals("acceptor-5"));
            cluster.network().deliverAll();
            TestSupport.await(() -> cluster.network().pendingMatching(
                    m -> m instanceof Message.Accepted) == 4);
            cluster.network().deliverAll();
            thread.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertThat(cluster.stats().isDecided()).isTrue();
        assertThat(cluster.stats().decidedValue()).isEqualTo("x");
        assertThat(cluster.acceptorCountWithValue("x")).isEqualTo(4);
    }
}
