package com.example.gsb.paxos;

import static org.assertj.core.api.Assertions.fail;

import java.util.function.BooleanSupplier;

/** 测试用轮询等待工具（避免引入额外依赖）。 */
final class TestSupport {

    private TestSupport() {
    }

    static void await(BooleanSupplier condition) {
        await(condition, 5_000);
    }

    static void await(BooleanSupplier condition, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted while waiting");
            }
        }
        if (!condition.getAsBoolean()) {
            fail("condition not met within " + timeoutMs + " ms");
        }
    }
}
