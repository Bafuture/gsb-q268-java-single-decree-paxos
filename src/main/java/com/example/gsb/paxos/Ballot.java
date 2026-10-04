package com.example.gsb.paxos;

import java.util.Objects;

/**
 * Paxos 提案编号（选票号）。
 *
 * <p>采用 {@code (round, proposerId)} 全序比较：先比轮次，轮次相同再比提案者 id。
 * 这样不同提案者即使从相同轮次开始，也不会产生相同编号的提案。
 */
public record Ballot(long round, String proposerId) implements Comparable<Ballot> {

    public Ballot {
        if (round < 0) {
            throw new IllegalArgumentException("round must be >= 0");
        }
        Objects.requireNonNull(proposerId, "proposerId");
    }

    @Override
    public int compareTo(Ballot other) {
        int byRound = Long.compare(round, other.round);
        return byRound != 0 ? byRound : proposerId.compareTo(other.proposerId);
    }

    @Override
    public String toString() {
        return "(" + round + "," + proposerId + ")";
    }
}
