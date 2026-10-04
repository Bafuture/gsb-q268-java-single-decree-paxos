package com.example.gsb.paxos;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 运行统计：提案轮次、各阶段消息数、被拒绝次数与最终达成的值。
 *
 * <p>线程安全，可被多个并发提案者与接受者共享。
 */
public class PaxosStats {

    private final AtomicInteger proposalRounds = new AtomicInteger();
    private final AtomicInteger prepareMessages = new AtomicInteger();
    private final AtomicInteger promiseMessages = new AtomicInteger();
    private final AtomicInteger acceptMessages = new AtomicInteger();
    private final AtomicInteger acceptedMessages = new AtomicInteger();
    private final AtomicInteger rejections = new AtomicInteger();
    private final AtomicReference<String> decidedValue = new AtomicReference<>();

    void roundStarted() {
        proposalRounds.incrementAndGet();
    }

    void messageSent(Message message) {
        if (message instanceof Message.Prepare) {
            prepareMessages.incrementAndGet();
        } else if (message instanceof Message.Promise) {
            promiseMessages.incrementAndGet();
        } else if (message instanceof Message.Accept) {
            acceptMessages.incrementAndGet();
        } else if (message instanceof Message.Accepted) {
            acceptedMessages.incrementAndGet();
        } else if (message instanceof Message.Reject) {
            rejections.incrementAndGet();
        }
    }

    /**
     * 记录一次多数派达成的决议。Paxos 安全性保证所有决议值相同；
     * 若出现冲突说明实现被破坏，直接抛错让测试失败。
     */
    public void recordDecision(String value) {
        if (!decidedValue.compareAndSet(null, value) && !decidedValue.get().equals(value)) {
            throw new IllegalStateException(
                    "conflicting decisions: " + decidedValue.get() + " vs " + value);
        }
    }

    /** 提案轮次（所有提案者发起的 prepare 轮数之和）。 */
    public int proposalRounds() {
        return proposalRounds.get();
    }

    /** Prepare 消息数（提案者发出）。 */
    public int prepareMessages() {
        return prepareMessages.get();
    }

    /** Promise 消息数（接受者回复）。 */
    public int promiseMessages() {
        return promiseMessages.get();
    }

    /** Accept 消息数（提案者发出）。 */
    public int acceptMessages() {
        return acceptMessages.get();
    }

    /** Accepted 消息数（接受者回复）。 */
    public int acceptedMessages() {
        return acceptedMessages.get();
    }

    /** prepare 阶段消息总数（Prepare + Promise）。 */
    public int preparePhaseMessages() {
        return prepareMessages.get() + promiseMessages.get();
    }

    /** accept 阶段消息总数（Accept + Accepted）。 */
    public int acceptPhaseMessages() {
        return acceptMessages.get() + acceptedMessages.get();
    }

    /** 被拒绝次数（接受者发出的 Reject 总数）。 */
    public int rejections() {
        return rejections.get();
    }

    /** 是否已有值被多数派接受并达成决议。 */
    public boolean isDecided() {
        return decidedValue.get() != null;
    }

    /** 达成的值；未达成时为 {@code null}。 */
    public String decidedValue() {
        return decidedValue.get();
    }

    @Override
    public String toString() {
        return "PaxosStats{rounds=" + proposalRounds()
                + ", preparePhase=" + preparePhaseMessages()
                + ", acceptPhase=" + acceptPhaseMessages()
                + ", rejections=" + rejections()
                + ", decided=" + decidedValue() + '}';
    }
}
