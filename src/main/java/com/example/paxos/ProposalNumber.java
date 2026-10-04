package com.example.paxos;

import java.util.Objects;

/**
 * 提案编号，全局全序：先比较轮次，再比较提案者标识（保证不同提案者的编号不会相等）。
 */
public record ProposalNumber(long round, String proposerId) implements Comparable<ProposalNumber> {

    public ProposalNumber {
        Objects.requireNonNull(proposerId, "proposerId");
    }

    @Override
    public int compareTo(ProposalNumber other) {
        int byRound = Long.compare(round, other.round);
        return byRound != 0 ? byRound : proposerId.compareTo(other.proposerId);
    }

    @Override
    public String toString() {
        return round + "@" + proposerId;
    }
}
