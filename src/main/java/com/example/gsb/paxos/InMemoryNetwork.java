package com.example.gsb.paxos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * 内存消息网络：负责在节点间投递消息并统计流量。
 *
 * <p>两种投递模式：
 * <ul>
 *   <li>立即投递（默认）：{@code send} 同步调用接收方处理器；</li>
 *   <li>排队投递：消息进入发件箱，由测试显式 {@link #deliverNext()}/{@link #deliverAll()}
 *       触发投递，可配合 {@link #dropNext()}、{@link #shuffle(long)} 模拟丢包、延迟与乱序。</li>
 * </ul>
 */
public class InMemoryNetwork {

    private final PaxosStats stats;
    private final Map<String, Consumer<Message>> handlers = new ConcurrentHashMap<>();
    private final List<Message> outbox = new ArrayList<>();
    private final Object outboxLock = new Object();
    private volatile boolean queueing;

    public InMemoryNetwork(PaxosStats stats) {
        this.stats = stats;
    }

    /** 注册节点消息处理器，{@code nodeId} 与消息的 {@code to} 对应。 */
    public void register(String nodeId, Consumer<Message> handler) {
        handlers.put(nodeId, handler);
    }

    /** 发送消息：立即投递或进入发件箱等待测试调度。 */
    public void send(Message message) {
        stats.messageSent(message);
        if (queueing) {
            synchronized (outboxLock) {
                outbox.add(message);
            }
        } else {
            dispatch(message);
        }
    }

    /** 开启/关闭排队模式。 */
    public void setQueueingEnabled(boolean enabled) {
        this.queueing = enabled;
    }

    /** 发件箱中等待投递的消息数。 */
    public int pending() {
        synchronized (outboxLock) {
            return outbox.size();
        }
    }

    /** 发件箱中满足条件的消息数。 */
    public int pendingMatching(Predicate<Message> predicate) {
        synchronized (outboxLock) {
            int count = 0;
            for (Message message : outbox) {
                if (predicate.test(message)) {
                    count++;
                }
            }
            return count;
        }
    }

    /** 发件箱快照（按入队顺序）。 */
    public List<Message> pendingMessages() {
        synchronized (outboxLock) {
            return List.copyOf(outbox);
        }
    }

    /** 投递队首消息；发件箱为空时返回 {@code false}。 */
    public boolean deliverNext() {
        Message message;
        synchronized (outboxLock) {
            if (outbox.isEmpty()) {
                return false;
            }
            message = outbox.remove(0);
        }
        dispatch(message);
        return true;
    }

    /**
     * 投递当前发件箱中的全部消息（快照语义：投递过程中新产生的回复留在发件箱，
     * 由下一次投递处理），返回投递数量。
     */
    public int deliverAll() {
        List<Message> batch;
        synchronized (outboxLock) {
            batch = new ArrayList<>(outbox);
            outbox.clear();
        }
        batch.forEach(this::dispatch);
        return batch.size();
    }

    /** 投递所有满足条件的消息（保持原有相对顺序），返回投递数量。 */
    public int deliverMatching(Predicate<Message> predicate) {
        List<Message> selected = new ArrayList<>();
        synchronized (outboxLock) {
            outbox.removeIf(message -> {
                if (predicate.test(message)) {
                    selected.add(message);
                    return true;
                }
                return false;
            });
        }
        selected.forEach(this::dispatch);
        return selected.size();
    }

    /** 丢弃队首消息（模拟丢包）；发件箱为空时返回 {@code false}。 */
    public boolean dropNext() {
        synchronized (outboxLock) {
            if (outbox.isEmpty()) {
                return false;
            }
            outbox.remove(0);
            return true;
        }
    }

    /** 丢弃所有满足条件的消息，返回丢弃数量。 */
    public int dropMatching(Predicate<Message> predicate) {
        synchronized (outboxLock) {
            int before = outbox.size();
            outbox.removeIf(predicate);
            return before - outbox.size();
        }
    }

    /** 丢弃全部排队消息。 */
    public int dropAll() {
        return dropMatching(message -> true);
    }

    /** 用固定种子打乱发件箱顺序（模拟乱序）。 */
    public void shuffle(long seed) {
        synchronized (outboxLock) {
            Collections.shuffle(outbox, new Random(seed));
        }
    }

    /** 打乱后全部投递（乱序 + 延迟场景一步到位）。 */
    public int deliverAllShuffled(long seed) {
        shuffle(seed);
        return deliverAll();
    }

    private void dispatch(Message message) {
        Consumer<Message> handler = handlers.get(message.to());
        if (handler != null) {
            handler.accept(message);
        }
    }
}
