package com.example.paxos;

/**
 * 单个提案者的运行统计：提案轮次、prepare/accept 消息数、被拒绝次数与最终达成的值。
 */
public class PaxosStats {

    private int rounds;
    private int prepareMessages;
    private int acceptMessages;
    private int rejections;
    private String decidedValue;

    void onNewRound() {
        rounds++;
    }

    void onPrepareSent() {
        prepareMessages++;
    }

    void onAcceptSent() {
        acceptMessages++;
    }

    void onRejected() {
        rejections++;
    }

    void onDecided(String value) {
        decidedValue = value;
    }

    /** 提案者进入过的 prepare 轮次数。 */
    public int getRounds() {
        return rounds;
    }

    /** 发送过的 prepare 消息总数。 */
    public int getPrepareMessages() {
        return prepareMessages;
    }

    /** 发送过的 accept 请求总数。 */
    public int getAcceptMessages() {
        return acceptMessages;
    }

    /** 收到的拒绝（Nack）总数。 */
    public int getRejections() {
        return rejections;
    }

    /** 达成的值；尚未达成时为 null。 */
    public String getDecidedValue() {
        return decidedValue;
    }

    @Override
    public String toString() {
        return "PaxosStats{rounds=" + rounds
                + ", prepareMessages=" + prepareMessages
                + ", acceptMessages=" + acceptMessages
                + ", rejections=" + rejections
                + ", decidedValue=" + decidedValue + '}';
    }
}
