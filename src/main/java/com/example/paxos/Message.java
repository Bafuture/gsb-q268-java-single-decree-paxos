package com.example.paxos;

/**
 * Paxos 消息。所有消息都是不可变的内存对象，由 {@link Network} 负责投递。
 */
public sealed interface Message {

    String from();

    String to();

    /** 阶段一：提案者请求接受者承诺编号 number。 */
    record Prepare(ProposalNumber number, String from, String to) implements Message {
    }

    /** 阶段一响应：接受者承诺 number，并顺带汇报自己已接受的编号与值（可能为空）。 */
    record Promise(ProposalNumber number, ProposalNumber acceptedNumber, String acceptedValue,
                   String from, String to) implements Message {
    }

    /** 阶段二：提案者请求接受者接受 (number, value)。 */
    record AcceptRequest(ProposalNumber number, String value, String from, String to) implements Message {
    }

    /** 阶段二响应：接受者已接受 (number, value)。 */
    record Accepted(ProposalNumber number, String value, String from, String to) implements Message {
    }

    /** 拒绝响应（prepare 或 accept 被拒），携带接受者当前已承诺的最高编号，便于提案者追赶。 */
    record Nack(ProposalNumber number, ProposalNumber promised, String from, String to) implements Message {
    }
}
