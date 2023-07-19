package com.rarible.blockchain.scanner.client

import com.rarible.blockchain.scanner.configuration.ClientRetryPolicyProperties
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import reactor.core.publisher.Mono
import java.io.IOException
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

class AbstractRetryableClientTest {

    private val retryPolicy = ClientRetryPolicyProperties(
        delay = Duration.ofMillis(10),
        increment = Duration.ofMillis(5),
        attempts = 3,
    )

    private val testClient: TestClient = mockk()
    private val client = RetryableTestClient(testClient, retryPolicy)

    @BeforeEach
    fun beforeEach() {
        clearMocks(testClient)
    }

    @ParameterizedTest
    @ValueSource(strings = ["mono", "flow", "suspend"])
    fun `retry - ok`(name: String) = runBlocking<Unit> {
        val count = AtomicInteger(0)
        val result = execute(name) { if (count.getAndIncrement() == 2) "test" else throw RuntimeException("Fail") }
        assertThat(result).isEqualTo("test")
        // 2 fails, 3rd is ok
        assertThat(count.get()).isEqualTo(3)
    }

    @ParameterizedTest
    @ValueSource(strings = ["mono", "flow", "suspend"])
    fun `retry - fail`(name: String) = runBlocking<Unit> {
        val count = AtomicInteger(0)

        assertThrows<IOException> {
            execute(name) {
                count.incrementAndGet()
                throw IOException("Fail")
            }
        }
        // Original attempt + 2 retries
        assertThat(count.get()).isEqualTo(3)
    }

    @ParameterizedTest
    @ValueSource(strings = ["mono", "flow", "suspend"])
    fun `retry - aborted`(name: String) = runBlocking<Unit> {
        val count = AtomicInteger(0)

        assertThrows<NonRetryableBlockchainClientException> {
            execute(name) {
                count.incrementAndGet()
                throw NonRetryableBlockchainClientException("Abort")
            }
        }
        // Should fail after first attempt
        assertThat(count.get()).isEqualTo(1)
    }

    private interface TestClient {
        fun monoMethod(): Mono<String>
        fun flowMethod(): Flow<String>
        suspend fun suspendMethod(): String
    }

    private suspend fun execute(name: String, block: () -> String): String {
        return when (name) {
            "flow" -> executeFlow(block)
            "mono" -> executeMono(block)
            "suspend" -> executeSuspend(block)
            else -> throw IllegalArgumentException("Ooops")
        }
    }

    private suspend fun executeMono(block: () -> String): String {
        every { testClient.monoMethod() } returns (run { mono { block() } })
        return client.monoMethod().awaitFirst()
    }

    private suspend fun executeFlow(block: () -> String): String {
        every { testClient.flowMethod() } returns (flow { emit(block()) })
        return client.flowMethod().first()
    }

    private suspend fun executeSuspend(block: () -> String): String {
        coEvery { testClient.suspendMethod() } answers { block() }
        return client.suspendMethod()
    }

    private class RetryableTestClient(
        private val testClient: TestClient,
        retryPolicyProperties: ClientRetryPolicyProperties
    ) : TestClient, AbstractRetryableClient(retryPolicyProperties) {
        override fun monoMethod(): Mono<String> {
            return testClient.monoMethod().wrapWithRetry()
        }

        override fun flowMethod(): Flow<String> {
            return testClient.flowMethod().wrapWithRetry()
        }

        override suspend fun suspendMethod(): String {
            return wrapWithRetry("") { testClient.suspendMethod() }
        }
    }
}