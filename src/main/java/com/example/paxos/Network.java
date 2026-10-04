package com.example.paxos;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Predicate;

/**
 * 内存消息网络：负责在提案者与接受者之间投递消息，并模拟真实网络的
 * 丢包（drop rule / dropMatching）、延迟（delay rule / delayMatching + releaseDelayed）
 * 与乱序（enableReordering）。
 *
 * <p>投递模型：{@link #send} 只入队，{@link #deliverAll} 把就绪队列中的消息逐条投递；
 * 投递过程中产生的响应会再次入队并在同一轮中被继续投递。被延迟的消息不参与投递，
 * 直到 {@link #releaseDelayed} 将其放回就绪队列。
 */
public class Network {

    private final Map<String, Acceptor> acceptors = new HashMap<>();
    private final Map<String, Proposer> proposers = new HashMap<>();
    private final Deque<Message> ready = new ArrayDeque<>();
    private final List<Message> delayed = new ArrayList<>();
    private Predicate<Message> dropRule = message -> false;
    private Predicate<Message> delayRule = message -> false;
    private Random reorderRandom;

    void registerAcceptor(Acceptor acceptor) {
        acceptors.put(acceptor.id(), acceptor);
    }

    void registerProposer(Proposer proposer) {
        proposers.put(proposer.id(), proposer);
    }

    /** 发送消息。命中丢包规则的消息被直接丢弃，命中延迟规则的消息进入延迟队列。 */
    public void send(Message message) {
        if (dropRule.test(message)) {
            return;
        }
        if (delayRule.test(message)) {
            delayed.add(message);
        } else {
            ready.add(message);
        }
    }

    /** 投递就绪队列中的全部消息（含投递过程中新产生的响应）。 */
    public void deliverAll() {
        while (!ready.isEmpty()) {
            if (reorderRandom != null && ready.size() > 1) {
                List<Message> batch = new ArrayList<>(ready);
                ready.clear();
                Collections.shuffle(batch, reorderRandom);
                ready.addAll(batch);
            }
            deliverOne(ready.poll());
        }
    }

    private void deliverOne(Message message) {
        Acceptor acceptor = acceptors.get(message.to());
        if (acceptor != null) {
            send(acceptor.handle(message));
            return;
        }
        Proposer proposer = proposers.get(message.to());
        if (proposer != null) {
            proposer.receive(message);
            return;
        }
        throw new IllegalStateException("unknown recipient: " + message.to());
    }

    /** 设置持续生效的丢包规则（模拟链路故障）。 */
    public void setDropRule(Predicate<Message> rule) {
        this.dropRule = rule;
    }

    /** 设置持续生效的延迟规则（模拟链路延迟）。 */
    public void setDelayRule(Predicate<Message> rule) {
        this.delayRule = rule;
    }

    /** 开启乱序投递：每轮投递前用给定随机源打乱就绪队列。 */
    public void enableReordering(Random random) {
        this.reorderRandom = random;
    }

    public void disableReordering() {
        this.reorderRandom = null;
    }

    /** 丢弃就绪队列中符合条件的消息，返回丢弃条数。 */
    public int dropMatching(Predicate<Message> predicate) {
        int before = ready.size();
        ready.removeIf(predicate);
        return before - ready.size();
    }

    /** 把就绪队列中符合条件的消息移入延迟队列。 */
    public void delayMatching(Predicate<Message> predicate) {
        List<Message> toDelay = ready.stream().filter(predicate).toList();
        ready.removeIf(predicate);
        delayed.addAll(toDelay);
    }

    /** 释放全部被延迟的消息（可选打乱顺序，模拟迟到消息乱序到达）。 */
    public void releaseDelayed(boolean shuffle, Random random) {
        if (shuffle) {
            Collections.shuffle(delayed, random);
        }
        ready.addAll(delayed);
        delayed.clear();
    }

    /** 就绪队列中尚未投递的消息数。 */
    public int pendingCount() {
        return ready.size();
    }

    /** 延迟队列中的消息数。 */
    public int delayedCount() {
        return delayed.size();
    }
}
