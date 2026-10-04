package com.example.paxos;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 测试与演示用的集群装配：持有一组接受者与一个共享的内存网络，
 * 可按需创建提案者。
 */
public class Cluster {

    private final Network network = new Network();
    private final Map<String, Acceptor> acceptors = new LinkedHashMap<>();

    public static Cluster withAcceptors(int count) {
        Cluster cluster = new Cluster();
        for (int i = 1; i <= count; i++) {
            cluster.addAcceptor("A" + i);
        }
        return cluster;
    }

    public void addAcceptor(String id) {
        Acceptor acceptor = new Acceptor(id);
        acceptors.put(id, acceptor);
        network.registerAcceptor(acceptor);
    }

    /** 创建一个面向当前全部接受者的提案者并注册到网络。 */
    public Proposer newProposer(String id) {
        Proposer proposer = new Proposer(id, acceptors.keySet().stream().toList(), network);
        network.registerProposer(proposer);
        return proposer;
    }

    public Acceptor acceptor(String id) {
        return acceptors.get(id);
    }

    public Collection<Acceptor> acceptors() {
        return acceptors.values();
    }

    public Network network() {
        return network;
    }
}
