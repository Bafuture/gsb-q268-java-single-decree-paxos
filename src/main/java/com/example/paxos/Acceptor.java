package com.example.paxos;

import com.example.paxos.Message.AcceptRequest;
import com.example.paxos.Message.Accepted;
import com.example.paxos.Message.Nack;
import com.example.paxos.Message.Prepare;
import com.example.paxos.Message.Promise;

/**
 * Paxos 接受者。持有两段持久状态：已承诺的最高编号、已接受的 (编号, 值)。
 *
 * <p><b>承诺规则（本组件安全性的核心）</b>：接受者收到编号 n 的 prepare 时，若 n 不低于自己
 * 已承诺的编号，则承诺 n，并保证此后拒绝一切编号更小的 prepare 与 accept。该规则的作用：
 * 一旦某个值在某编号下被多数派接受，任何更高编号的提案者在 prepare 阶段都必然与这个多数派
 * 相交，从而通过 Promise 中汇报的“已接受值”发现旧值并沿用之——已达成的决议永远不会被
 * 更高编号的提案覆盖。换言之，承诺规则把“编号大的提案优先”转化为“先被多数派接受的值优先”，
 * 这是 Paxos 在并发提案者与提案者崩溃场景下仍能保证一致性的关键。
 */
public class Acceptor {

    private final String id;
    private ProposalNumber promised;
    private ProposalNumber acceptedNumber;
    private String acceptedValue;

    public Acceptor(String id) {
        this.id = id;
    }

    /** 处理一条入站消息并产生响应（不会返回 null）。 */
    public Message handle(Message message) {
        if (message instanceof Prepare prepare) {
            return onPrepare(prepare);
        }
        if (message instanceof AcceptRequest accept) {
            return onAccept(accept);
        }
        throw new IllegalArgumentException("acceptor cannot handle " + message);
    }

    private Message onPrepare(Prepare prepare) {
        if (promised == null || prepare.number().compareTo(promised) >= 0) {
            promised = prepare.number();
            return new Promise(prepare.number(), acceptedNumber, acceptedValue, id, prepare.from());
        }
        // 已承诺过更高编号：拒绝，并告知当前承诺编号让提案者追赶
        return new Nack(prepare.number(), promised, id, prepare.from());
    }

    private Message onAccept(AcceptRequest accept) {
        if (promised == null || accept.number().compareTo(promised) >= 0) {
            promised = accept.number();
            acceptedNumber = accept.number();
            acceptedValue = accept.value();
            return new Accepted(accept.number(), accept.value(), id, accept.from());
        }
        // 承诺规则的执行点：拒绝低于已承诺编号的 accept
        return new Nack(accept.number(), promised, id, accept.from());
    }

    public String id() {
        return id;
    }

    /** 已承诺的最高编号；尚未承诺过为 null。 */
    public ProposalNumber promised() {
        return promised;
    }

    /** 已接受的提案编号；尚未接受过为 null。 */
    public ProposalNumber acceptedNumber() {
        return acceptedNumber;
    }

    /** 已接受的值；尚未接受过为 null。 */
    public String acceptedValue() {
        return acceptedValue;
    }
}
