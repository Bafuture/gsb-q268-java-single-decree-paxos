package com.example.gsb.paxos;

/**
 * Paxos 协议消息（内存对象模拟）。
 *
 * <p>两阶段流程：
 * <ol>
 *   <li>Prepare 阶段：提案者广播 {@link Prepare}，接受者回复 {@link Promise} 或 {@link Reject}；</li>
 *   <li>Accept 阶段：提案者广播 {@link Accept}，接受者回复 {@link Accepted} 或 {@link Reject}。</li>
 * </ol>
 */
public sealed interface Message {

    /** 发送方节点 id。 */
    String from();

    /** 接收方节点 id。 */
    String to();

    /** 该消息所属的提案编号。 */
    Ballot ballot();

    /** 消息所属阶段，用于区分拒绝发生的阶段。 */
    enum Phase {
        PREPARE,
        ACCEPT
    }

    /** 阶段一：提案者请求接受者承诺编号 {@code ballot}。 */
    record Prepare(String from, String to, Ballot ballot) implements Message {
    }

    /**
     * 阶段一回复：接受者承诺不再接受编号低于 {@code ballot} 的提案，
     * 并顺带汇报自己已接受过的 {@code acceptedBallot}/{@code acceptedValue}（可能为空），
     * 供提案者做“沿用旧值”的选择。
     */
    record Promise(String from, String to, Ballot ballot, Ballot acceptedBallot, String acceptedValue)
            implements Message {
    }

    /** 阶段二：提案者请求接受者接受 {@code (ballot, value)}。 */
    record Accept(String from, String to, Ballot ballot, String value) implements Message {
    }

    /** 阶段二回复：接受者已接受 {@code (ballot, value)}。 */
    record Accepted(String from, String to, Ballot ballot, String value) implements Message {
    }

    /**
     * 任一阶段的拒绝：接受者已承诺过更高的编号 {@code promisedBallot}，
     * 提案者可借此快速抬高自己的下一轮编号。
     */
    record Reject(String from, String to, Ballot ballot, Ballot promisedBallot, Phase phase)
            implements Message {
    }
}
