package com.rarible.blockchain.scanner.client

import com.rarible.blockchain.scanner.configuration.ClientRetryPolicyProperties
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import reactor.core.publisher.Mono
import java.io.IOException
import java.lang.RuntimeException
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

class AbstractRetryableClientTest {

    private val retryPolicy = ClientRetryPolicyProperties(
        delay = Duration.ofMillis(10),
        increment = Duration.ofMillis(5),
        attempts = 3,
    )

    @Test
    fun `retry ok`() = runBlocking<Unit> {
        val count = AtomicLong(1)
        val testClient = mockk<TestClient> {
            every { method() } returns (
                run {
                    mono {
                        if (count.getAndIncrement() > 2) {
                            "test"
                        } else {
                            throw RuntimeException("Buy")
                        }
                    }
                }
            )
        }
        val retryClient = RetryableTestClient(testClient, retryPolicy)
        val result = retryClient.method().awaitFirst()
        assertThat(result).isEqualTo("test")
        assertThat(count.get()).isEqualTo(3 + 1)
    }

    @Test
    fun `retry fail`() = runBlocking<Unit> {
        val testClient = mockk<TestClient> {
            every { method() } returns (
                    run {
                        mono {
                            throw IOException("Buy")
                        }
                    }
            )
        }
        val retryClient = RetryableTestClient(testClient, retryPolicy)
        assertThrows<IOException> {
            runBlocking {
                retryClient.method().awaitFirst()
            }
        }
    }

    private interface TestClient {
        fun method(): Mono<String>
    }

    private class RetryableTestClient(
        private val testClient: TestClient,
        retryPolicyProperties: ClientRetryPolicyProperties
    ) : TestClient, AbstractRetryableClient(retryPolicyProperties) {
        override fun method(): Mono<String> {
            return testClient.method().wrapWithRetry()
        }
    }
}