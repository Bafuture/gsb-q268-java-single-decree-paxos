package com.example.gsb.paxos;

/**
 * Paxos 接受者。
 *
 * <p>核心不变式（承诺规则）：一旦对编号 {@code b} 做出承诺，就拒绝一切编号小于
 * {@code b} 的 prepare/accept。该规则保证“已被多数派接受的值”不会被后续更小编号
 * 的提案覆盖——任何更高的提案在 prepare 阶段都能从多数派中打听到已接受的旧值并沿用之，
 * 这是 Paxos 安全性的基石。
 */
public class Acceptor {

    private final String id;
    private final InMemoryNetwork network;

    /** 已承诺的最高编号；{@code null} 表示尚未承诺。 */
    private Ballot promisedBallot;
    /** 已接受的提案编号；{@code null} 表示尚未接受任何值。 */
    private Ballot acceptedBallot;
    /** 已接受的提案值。 */
    private String acceptedValue;

    public Acceptor(String id, InMemoryNetwork network) {
        this.id = id;
        this.network = network;
        network.register(id, this::onMessage);
    }

    public synchronized void onMessage(Message message) {
        if (message instanceof Message.Prepare prepare) {
            handlePrepare(prepare);
        } else if (message instanceof Message.Accept accept) {
            handleAccept(accept);
        }
    }

    private void handlePrepare(Message.Prepare prepare) {
        // 编号不小于已承诺编号时才给予承诺（允许等号：同一提案重试是幂等的）。
        if (promisedBallot == null || prepare.ballot().compareTo(promisedBallot) >= 0) {
            promisedBallot = prepare.ballot();
            send(new Message.Promise(id, prepare.from(), prepare.ballot(), acceptedBallot, acceptedValue));
        } else {
            send(new Message.Reject(id, prepare.from(), prepare.ballot(), promisedBallot,
                    Message.Phase.PREPARE));
        }
    }

    private void handleAccept(Message.Accept accept) {
        // 只接受编号不小于已承诺编号的提案；接受的同时也更新承诺编号。
        if (promisedBallot == null || accept.ballot().compareTo(promisedBallot) >= 0) {
            promisedBallot = accept.ballot();
            acceptedBallot = accept.ballot();
            acceptedValue = accept.value();
            send(new Message.Accepted(id, accept.from(), accept.ballot(), accept.value()));
        } else {
            send(new Message.Reject(id, accept.from(), accept.ballot(), promisedBallot,
                    Message.Phase.ACCEPT));
        }
    }

    private void send(Message message) {
        network.send(message);
    }

    public String id() {
        return id;
    }

    public synchronized Ballot promisedBallot() {
        return promisedBallot;
    }

    public synchronized Ballot acceptedBallot() {
        return acceptedBallot;
    }

    public synchronized String acceptedValue() {
        return acceptedValue;
    }
}
