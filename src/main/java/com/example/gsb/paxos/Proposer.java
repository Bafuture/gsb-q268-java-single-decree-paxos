package com.example.gsb.paxos;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Paxos 提案者，实现 prepare/accept 两阶段协议。
 *
 * <p>安全性要点：
 * <ul>
 *   <li>仅在收到多数派的 Promise 后才进入 accept 阶段；</li>
 *   <li>Promise 中若带了接受者已接受的旧值，必须沿用其中编号最高的旧值，
 *       绝不能用自己的值覆盖（否则可能推翻多数派已经接受的决议）；</li>
 *   <li>收到 Reject 后抬高编号重试。</li>
 * </ul>
 *
 * <p>崩溃模型：{@link #crash()} 立即中止当前提案并丢弃后续消息；对象中持久化的
 * {@code nextRound} 在 {@link #recover()} 后继续使用，下一轮编号严格更大，
 * 因此恢复后重新提案只会重新走两阶段并沿用旧值，不违反已达成的一致性。
 */
public class Proposer {

    private final String id;
    private final List<String> acceptorIds;
    private final int quorum;
    private final InMemoryNetwork network;
    private final PaxosStats stats;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();

    private volatile boolean crashed;
    private long nextRound = 1;

    private Duration phaseTimeout = Duration.ofSeconds(1);
    private int maxRounds = 10;
    private long retryBackoffMaxMs = 20;

    public Proposer(String id, List<String> acceptorIds, InMemoryNetwork network, PaxosStats stats) {
        this.id = Objects.requireNonNull(id, "id");
        this.acceptorIds = List.copyOf(acceptorIds);
        this.quorum = this.acceptorIds.size() / 2 + 1;
        this.network = Objects.requireNonNull(network, "network");
        this.stats = Objects.requireNonNull(stats, "stats");
        network.register(id, this::onMessage);
    }

    public void setPhaseTimeout(Duration phaseTimeout) {
        this.phaseTimeout = Objects.requireNonNull(phaseTimeout);
    }

    public void setMaxRounds(int maxRounds) {
        if (maxRounds <= 0) {
            throw new IllegalArgumentException("maxRounds must be positive");
        }
        this.maxRounds = maxRounds;
    }

    public void setRetryBackoffMaxMs(long retryBackoffMaxMs) {
        this.retryBackoffMaxMs = retryBackoffMaxMs;
    }

    private void onMessage(Message message) {
        if (!crashed) {
            inbox.offer(message);
        }
    }

    /**
     * 反复发起编号递增的两阶段提案，直到达成决议、被 {@link #crash()} 中止，
     * 或用尽 {@code maxRounds} 轮仍未获得多数派。
     */
    public ProposalResult propose(String ownValue) {
        Objects.requireNonNull(ownValue, "ownValue");
        for (int attempt = 0; attempt < maxRounds; attempt++) {
            if (crashed) {
                return ProposalResult.crashed();
            }
            Ballot ballot = new Ballot(nextRound++, id);
            stats.roundStarted();

            // ---- 阶段一：prepare，收集多数派承诺 ----
            for (String acceptor : acceptorIds) {
                network.send(new Message.Prepare(id, acceptor, ballot));
            }
            PhaseOne phaseOne = awaitPhaseOne(ballot);
            if (phaseOne == null) {
                return ProposalResult.crashed();
            }
            if (phaseOne.promises.size() < quorum) {
                if (!backoff()) {
                    return ProposalResult.crashed();
                }
                continue;
            }

            // ---- 旧值沿用：取已接受编号最高的值，没有旧值才用自己的值 ----
            String chosenValue = chooseValue(phaseOne.promises, ownValue);

            // ---- 阶段二：accept，收集多数派接受确认 ----
            for (String acceptor : acceptorIds) {
                network.send(new Message.Accept(id, acceptor, ballot, chosenValue));
            }
            PhaseTwo phaseTwo = awaitPhaseTwo(ballot);
            if (phaseTwo == null) {
                return ProposalResult.crashed();
            }
            if (phaseTwo.accepted.size() >= quorum) {
                stats.recordDecision(chosenValue);
                return ProposalResult.decided(chosenValue);
            }

            if (!backoff()) {
                return ProposalResult.crashed();
            }
        }
        return ProposalResult.notDecided();
    }

    /** 标记提案者崩溃：当前提案尽快中止，期间到达的消息一律丢弃。 */
    public void crash() {
        crashed = true;
    }

    /** 从崩溃中恢复；下一轮提案使用严格更大的编号。 */
    public void recover() {
        crashed = false;
    }

    public String id() {
        return id;
    }

    private PhaseOne awaitPhaseOne(Ballot ballot) {
        List<Message.Promise> promises = new ArrayList<>();
        Set<String> responded = new HashSet<>();
        long deadline = System.nanoTime() + phaseTimeout.toNanos();
        while (promises.size() < quorum && responded.size() < acceptorIds.size()) {
            if (crashed) {
                return null;
            }
            Message message = pollUntil(deadline);
            if (message == null) {
                break; // 超时
            }
            if (message instanceof Message.Promise promise && promise.ballot().equals(ballot)) {
                promises.add(promise);
                responded.add(promise.from());
            } else if (message instanceof Message.Reject reject
                    && reject.ballot().equals(ballot)
                    && reject.phase() == Message.Phase.PREPARE) {
                responded.add(reject.from());
                adoptHigherRound(reject.promisedBallot());
            }
        }
        return new PhaseOne(promises);
    }

    private PhaseTwo awaitPhaseTwo(Ballot ballot) {
        List<Message.Accepted> accepted = new ArrayList<>();
        Set<String> responded = new HashSet<>();
        long deadline = System.nanoTime() + phaseTimeout.toNanos();
        while (accepted.size() < quorum && responded.size() < acceptorIds.size()) {
            if (crashed) {
                return null;
            }
            Message message = pollUntil(deadline);
            if (message == null) {
                break; // 超时
            }
            if (message instanceof Message.Accepted ack && ack.ballot().equals(ballot)) {
                accepted.add(ack);
                responded.add(ack.from());
            } else if (message instanceof Message.Reject reject
                    && reject.ballot().equals(ballot)
                    && reject.phase() == Message.Phase.ACCEPT) {
                responded.add(reject.from());
                adoptHigherRound(reject.promisedBallot());
            }
        }
        return new PhaseTwo(accepted);
    }

    private Message pollUntil(long deadlineNanos) {
        long remainingMillis = Math.max(1, (deadlineNanos - System.nanoTime()) / 1_000_000L);
        long sleepMillis = Math.min(50L, remainingMillis);
        try {
            return inbox.poll(sleepMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private void adoptHigherRound(Ballot promisedBallot) {
        nextRound = Math.max(nextRound, promisedBallot.round() + 1);
    }

    /**
     * Paxos 提案值选择规则：多数派承诺中若有任何接受者汇报了已接受的值，
     * 必须选择其中“已接受编号”最大的那个值；都没有时才提议自己的值。
     */
    private String chooseValue(List<Message.Promise> promises, String ownValue) {
        Ballot highestBallot = null;
        String highestValue = null;
        for (Message.Promise promise : promises) {
            if (promise.acceptedValue() == null) {
                continue;
            }
            if (highestBallot == null || promise.acceptedBallot().compareTo(highestBallot) > 0) {
                highestBallot = promise.acceptedBallot();
                highestValue = promise.acceptedValue();
            }
        }
        return highestValue != null ? highestValue : ownValue;
    }

    private boolean backoff() {
        if (retryBackoffMaxMs <= 0) {
            return !crashed;
        }
        long waitMs = ThreadLocalRandom.current().nextLong(1, retryBackoffMaxMs + 1);
        long deadline = System.nanoTime() + waitMs * 1_000_000L;
        while (!crashed && System.nanoTime() < deadline) {
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return !crashed;
    }

    private record PhaseOne(List<Message.Promise> promises) {
    }

    private record PhaseTwo(List<Message.Accepted> accepted) {
    }
}
