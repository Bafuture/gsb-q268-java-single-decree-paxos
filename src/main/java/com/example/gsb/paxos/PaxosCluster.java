package com.example.gsb.paxos;

import java.util.List;
import java.util.stream.IntStream;

/**
 * 单决议 Paxos 集群装配器：创建接受者、网络与共享统计，并提供多数派校验辅助方法。
 */
public class PaxosCluster {

    private final PaxosStats stats = new PaxosStats();
    private final InMemoryNetwork network = new InMemoryNetwork(stats);
    private final List<Acceptor> acceptors;
    private final List<String> acceptorIds;

    public PaxosCluster(int acceptorCount) {
        if (acceptorCount <= 0 || acceptorCount % 2 == 0) {
            throw new IllegalArgumentException("acceptorCount must be a positive odd number");
        }
        this.acceptors = IntStream.rangeClosed(1, acceptorCount)
                .mapToObj(i -> new Acceptor("acceptor-" + i, network))
                .toList();
        this.acceptorIds = acceptors.stream().map(Acceptor::id).toList();
    }

    /** 新增一个挂在该集群上的提案者。 */
    public Proposer newProposer(String proposerId) {
        return new Proposer(proposerId, acceptorIds, network, stats);
    }

    public int quorum() {
        return acceptors.size() / 2 + 1;
    }

    public List<Acceptor> acceptors() {
        return acceptors;
    }

    public PaxosStats stats() {
        return stats;
    }

    public InMemoryNetwork network() {
        return network;
    }

    /** 已接受 {@code value} 的接受者数量。 */
    public long acceptorCountWithValue(String value) {
        return acceptors.stream()
                .filter(acceptor -> value.equals(acceptor.acceptedValue()))
                .count();
    }

    /** 当前被多数派接受的值；没有值达到多数派时返回 {@code null}。 */
    public String majorityAcceptedValue() {
        for (Acceptor acceptor : acceptors) {
            String value = acceptor.acceptedValue();
            if (value != null && acceptorCountWithValue(value) >= quorum()) {
                return value;
            }
        }
        return null;
    }
}
