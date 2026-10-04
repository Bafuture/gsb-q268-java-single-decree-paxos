package com.example.gsb.paxos;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** 需求 4：并发提案者竞争——最终只能有一个值被多数派接受。 */
class ConcurrentProposersTest {

    @Test
    void concurrentProposersConvergeOnSingleValue() throws Exception {
        PaxosCluster cluster = new PaxosCluster(5);
        List<String> values = List.of("red", "green", "blue");
        ExecutorService executor = Executors.newFixedThreadPool(values.size());
        try {
            List<Future<ProposalResult>> futures = values.stream()
                    .map(value -> executor.submit(() -> {
                        Proposer proposer = cluster.newProposer("p-" + value);
                        return proposer.propose(value);
                    }))
                    .toList();

            List<ProposalResult> results = futures.stream()
                    .map(future -> {
                        try {
                            return future.get(30, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            throw new AssertionError("proposer did not finish", e);
                        }
                    })
                    .toList();

            // 至少一个提案者达成决议
            List<ProposalResult> decided = results.stream()
                    .filter(ProposalResult::isDecided)
                    .toList();
            assertThat(decided).isNotEmpty();

            // 所有达成的决议必须是同一个值
            String decidedValue = decided.get(0).value();
            assertThat(decided).allSatisfy(r -> assertThat(r.value()).isEqualTo(decidedValue));

            // 该值被多数派接受，且不存在另一个被多数派接受的值
            assertThat(cluster.majorityAcceptedValue()).isEqualTo(decidedValue);
            assertThat(cluster.acceptorCountWithValue(decidedValue))
                    .isGreaterThanOrEqualTo(cluster.quorum());
            for (String value : values) {
                if (!value.equals(decidedValue)) {
                    assertThat(cluster.acceptorCountWithValue(value))
                            .isLessThan(cluster.quorum());
                }
            }

            // 统计一致：全局决议值与多数派接受的值相同
            assertThat(cluster.stats().isDecided()).isTrue();
            assertThat(cluster.stats().decidedValue()).isEqualTo(decidedValue);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void repeatedConcurrentRunsNeverProduceConflictingDecisions() throws Exception {
        // 多跑几轮提高并发交错覆盖率；recordDecision 遇到冲突会直接抛错
        for (int i = 0; i < 5; i++) {
            PaxosCluster cluster = new PaxosCluster(3);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                Future<ProposalResult> f1 = executor.submit(
                        () -> cluster.newProposer("pa").propose("A"));
                Future<ProposalResult> f2 = executor.submit(
                        () -> cluster.newProposer("pb").propose("B"));
                ProposalResult r1 = f1.get(30, TimeUnit.SECONDS);
                ProposalResult r2 = f2.get(30, TimeUnit.SECONDS);

                assertThat(cluster.stats().isDecided()).isTrue();
                String decided = cluster.stats().decidedValue();
                if (r1.isDecided()) {
                    assertThat(r1.value()).isEqualTo(decided);
                }
                if (r2.isDecided()) {
                    assertThat(r2.value()).isEqualTo(decided);
                }
                assertThat(cluster.majorityAcceptedValue()).isEqualTo(decided);
            } finally {
                executor.shutdownNow();
            }
        }
    }
}
