package com.example.gsb.paxos;

/** 一次提案尝试的最终结果。 */
public record ProposalResult(Status status, String value) {

    public enum Status {
        /** 已获得多数派接受，{@code value} 为达成的值。 */
        DECIDED,
        /** 用尽重试轮次仍未获得多数派（例如消息全部丢失），未达成决议。 */
        NOT_DECIDED,
        /** 提案者中途崩溃，本次尝试中止。 */
        CRASHED
    }

    public static ProposalResult decided(String value) {
        return new ProposalResult(Status.DECIDED, value);
    }

    public static ProposalResult notDecided() {
        return new ProposalResult(Status.NOT_DECIDED, null);
    }

    public static ProposalResult crashed() {
        return new ProposalResult(Status.CRASHED, null);
    }

    public boolean isDecided() {
        return status == Status.DECIDED;
    }
}
