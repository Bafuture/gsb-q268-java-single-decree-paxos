package com.example.paxos;

import com.example.paxos.Message.AcceptRequest;
import com.example.paxos.Message.Accepted;
import com.example.paxos.Message.Nack;
import com.example.paxos.Message.Prepare;
import com.example.paxos.Message.Promise;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

/**
 * Paxos 提案者。两阶段流程：
 * <ol>
 *   <li>{@link #preparePhase()}：选更高的提案编号，向全部接受者发 prepare，
 *       收集多数派承诺；</li>
 *   <li>{@link #acceptPhase(String)}：按“沿用已接受编号最高的值，否则用自己的值”
 *       的规则选定提案值，向全部接受者发 accept，多数派接受即决议完成。</li>
 * </ol>
 *
 * <p>崩溃语义：{@link #crash()} 丢弃全部易失状态（收件箱、已收集的承诺），
 * 但保留自己上次使用的轮次（相当于提案者本地的稳定存储），保证恢复后不会复用旧编号。
 * 接受者侧状态不受提案者崩溃影响，因此恢复后重新提案不会破坏已达成的一致性。
 */
public class Proposer {

    /** 一次提案的最终结果。 */
    public record Outcome(boolean decided, String value) {

        public static Outcome decided(String value) {
            return new Outcome(true, value);
        }

        public static Outcome undecided() {
            return new Outcome(false, null);
        }
    }

    private final String id;
    private final List<String> acceptorIds;
    private final int majority;
    private final Network network;
    private final PaxosStats stats = new PaxosStats();

    // —— 崩溃后仍保留的状态（提案者本地稳定存储）——
    private long lastRound;

    // —— 易失状态，crash() 时全部丢失 ——
    private final Deque<Message> inbox = new ArrayDeque<>();
    private final List<Promise> promises = new ArrayList<>();
    private ProposalNumber currentNumber;

    public Proposer(String id, List<String> acceptorIds, Network network) {
        this.id = id;
        this.acceptorIds = List.copyOf(acceptorIds);
        this.majority = acceptorIds.size() / 2 + 1;
        this.network = network;
    }

    /**
     * 阶段一：开启新一轮（编号递增），向所有接受者发 prepare 并收集响应。
     *
     * @return 是否获得多数派承诺
     */
    public boolean preparePhase() {
        lastRound++;
        currentNumber = new ProposalNumber(lastRound, id);
        stats.onNewRound();
        promises.clear();
        for (String acceptorId : acceptorIds) {
            network.send(new Prepare(currentNumber, id, acceptorId));
            stats.onPrepareSent();
        }
        network.deliverAll();
        drainInbox();
        return promises.size() >= majority;
    }

    /**
     * 阶段二：按规则选定提案值并向所有接受者发 accept。
     *
     * @param ownValue 提案者自己希望提出的值（仅在没有任何已接受旧值时使用）
     * @return 若多数派接受则返回达成的值，否则为空
     */
    public Optional<String> acceptPhase(String ownValue) {
        if (currentNumber == null) {
            throw new IllegalStateException("acceptPhase requires a preceding preparePhase");
        }
        String value = chooseValue(ownValue);
        for (String acceptorId : acceptorIds) {
            network.send(new AcceptRequest(currentNumber, value, id, acceptorId));
            stats.onAcceptSent();
        }
        network.deliverAll();
        int acceptedCount = 0;
        while (!inbox.isEmpty()) {
            Message message = inbox.poll();
            if (message instanceof Accepted accepted && accepted.number().equals(currentNumber)) {
                acceptedCount++;
            } else if (message instanceof Nack nack && nack.number().equals(currentNumber)) {
                stats.onRejected();
                catchUp(nack);
            }
            // 迟到/乱序的旧编号响应直接忽略
        }
        if (acceptedCount >= majority) {
            stats.onDecided(value);
            return Optional.of(value);
        }
        return Optional.empty();
    }

    /**
     * 完整提案流程：反复执行两阶段，直到达成决议或耗尽重试次数。
     * 消息丢失、被拒绝等情况都会通过开启更高编号的新轮次来重试。
     */
    public Outcome propose(String value, int maxRounds) {
        for (int attempt = 0; attempt < maxRounds; attempt++) {
            if (preparePhase()) {
                Optional<String> decided = acceptPhase(value);
                if (decided.isPresent()) {
                    return Outcome.decided(decided.get());
                }
            }
        }
        return Outcome.undecided();
    }

    /** 模拟崩溃：清空收件箱与已收集的承诺；轮次计数保留，恢复后不会复用旧编号。 */
    public void crash() {
        inbox.clear();
        promises.clear();
        currentNumber = null;
    }

    /**
     * 提案值选择规则（安全性关键）：若承诺中携带了已被接受过的旧值，
     * 必须沿用其中编号最高的那个值，绝不允许用自己的值覆盖。
     */
    String chooseValue(String ownValue) {
        Promise best = null;
        for (Promise promise : promises) {
            if (promise.acceptedNumber() != null
                    && (best == null || promise.acceptedNumber().compareTo(best.acceptedNumber()) > 0)) {
                best = promise;
            }
        }
        return best == null ? ownValue : best.acceptedValue();
    }

    private void drainInbox() {
        while (!inbox.isEmpty()) {
            Message message = inbox.poll();
            if (message instanceof Promise promise && promise.number().equals(currentNumber)) {
                promises.add(promise);
            } else if (message instanceof Nack nack && nack.number().equals(currentNumber)) {
                stats.onRejected();
                catchUp(nack);
            }
            // 迟到/乱序的旧编号响应直接忽略
        }
    }

    /** 根据 Nack 中接受者已承诺的编号推进自己的轮次，保证下一轮编号更高。 */
    private void catchUp(Nack nack) {
        if (nack.promised() != null && nack.promised().round() > lastRound) {
            lastRound = nack.promised().round();
        }
    }

    void receive(Message message) {
        inbox.add(message);
    }

    public String id() {
        return id;
    }

    public PaxosStats stats() {
        return stats;
    }

    /** 当前轮次的提案编号（最近一次 preparePhase 选定）；崩溃后或未开始为 null。 */
    public ProposalNumber currentNumber() {
        return currentNumber;
    }
}
