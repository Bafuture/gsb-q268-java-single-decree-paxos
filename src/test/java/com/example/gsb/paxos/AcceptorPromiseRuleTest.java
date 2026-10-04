package com.example.gsb.paxos;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * 需求 2：承诺规则——接受者承诺某编号后，拒绝一切更低编号的 prepare/accept。
 *
 * <p>该规则的作用：一旦某个值被多数派接受，任何更小编号的提案都无法再让多数派
 * 接受别的值；而更大编号的提案在 prepare 阶段必然能从多数派打听到已接受的旧值
 * 并沿用之，从而保证单决议 Paxos 的安全性（不会先后决议出两个不同的值）。
 */
class AcceptorPromiseRuleTest {

    private PaxosStats stats;
    private InMemoryNetwork network;
    private List<Message> replies;
    private Acceptor acceptor;

    @BeforeEach
    void setUp() {
        stats = new PaxosStats();
        network = new InMemoryNetwork(stats);
        replies = new CopyOnWriteArrayList<>();
        network.register("p1", replies::add);
        acceptor = new Acceptor("a1", network);
    }

    private Message lastReply() {
        return replies.get(replies.size() - 1);
    }

    @Test
    void promisesHigherBallotAndRejectsLowerOnes() {
        // 首次 prepare：承诺
        acceptor.onMessage(new Message.Prepare("p1", "a1", new Ballot(5, "p1")));
        assertThat(lastReply()).isInstanceOf(Message.Promise.class);
        assertThat(acceptor.promisedBallot()).isEqualTo(new Ballot(5, "p1"));

        // 更低编号的 prepare：拒绝
        acceptor.onMessage(new Message.Prepare("p1", "a1", new Ballot(3, "p1")));
        assertThat(lastReply()).isInstanceOf(Message.Reject.class);
        Message.Reject prepareReject = (Message.Reject) lastReply();
        assertThat(prepareReject.phase()).isEqualTo(Message.Phase.PREPARE);
        assertThat(prepareReject.promisedBallot()).isEqualTo(new Ballot(5, "p1"));
        assertThat(acceptor.promisedBallot()).isEqualTo(new Ballot(5, "p1"));

        // 更低编号的 accept：拒绝，且不接受该值
        acceptor.onMessage(new Message.Accept("p1", "a1", new Ballot(4, "p1"), "stale"));
        assertThat(lastReply()).isInstanceOf(Message.Reject.class);
        assertThat(((Message.Reject) lastReply()).phase()).isEqualTo(Message.Phase.ACCEPT);
        assertThat(acceptor.acceptedValue()).isNull();

        // 更高编号的 prepare：重新承诺
        acceptor.onMessage(new Message.Prepare("p1", "a1", new Ballot(6, "p1")));
        assertThat(lastReply()).isInstanceOf(Message.Promise.class);
        assertThat(acceptor.promisedBallot()).isEqualTo(new Ballot(6, "p1"));

        // 达到承诺编号的 accept：接受
        acceptor.onMessage(new Message.Accept("p1", "a1", new Ballot(6, "p1"), "v6"));
        assertThat(lastReply()).isInstanceOf(Message.Accepted.class);
        assertThat(acceptor.acceptedValue()).isEqualTo("v6");
        assertThat(acceptor.acceptedBallot()).isEqualTo(new Ballot(6, "p1"));

        assertThat(stats.rejections()).isEqualTo(2);
    }

    @Test
    void promiseCarriesPreviouslyAcceptedValue() {
        acceptor.onMessage(new Message.Accept("p1", "a1", new Ballot(2, "p1"), "old"));
        assertThat(acceptor.acceptedValue()).isEqualTo("old");

        // 新一轮 prepare 的 Promise 必须带上已接受的旧值，供提案者沿用
        acceptor.onMessage(new Message.Prepare("p1", "a1", new Ballot(3, "p1")));
        Message.Promise promise = (Message.Promise) lastReply();
        assertThat(promise.acceptedBallot()).isEqualTo(new Ballot(2, "p1"));
        assertThat(promise.acceptedValue()).isEqualTo("old");
    }

    @Test
    void retriedPrepareWithSameBallotIsIdempotent() {
        // 网络丢包后提案者用同一编号重试 prepare，应再次得到承诺（幂等）
        acceptor.onMessage(new Message.Prepare("p1", "a1", new Ballot(5, "p1")));
        acceptor.onMessage(new Message.Prepare("p1", "a1", new Ballot(5, "p1")));
        assertThat(lastReply()).isInstanceOf(Message.Promise.class);
        assertThat(stats.rejections()).isZero();
    }
}
