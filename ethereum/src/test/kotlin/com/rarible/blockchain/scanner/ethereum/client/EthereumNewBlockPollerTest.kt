package com.rarible.blockchain.scanner.ethereum.client

import io.daonomic.rpc.domain.Word
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import scalether.core.MonoEthereum
import scalether.domain.response.Block
import java.math.BigInteger
import java.time.Duration

@Suppress("ReactiveStreamsUnusedPublisher")
internal class EthereumNewBlockPollerTest {
    private val ethereum = mockk<MonoEthereum>()
    private val pollingDelay = Duration.ZERO

    @ExperimentalCoroutinesApi
    private val poller = EthereumNewBlockPoller(ethereum, pollingDelay)

    @ExperimentalCoroutinesApi
    @Test
    fun polling() = runBlocking<Unit> {
        val blockNumber1 = BigInteger("1")
        val blockNumber2 = BigInteger("2")
        val block1 = mockk<Block<Word>> {
            every { number() } returns BigInteger.ONE
        }
        val block2 = mockk<Block<Word>> {
            every { number() } returns BigInteger.TEN
        }
        every { ethereum.ethBlockNumber() } returnsMany listOf(
            Mono.just(blockNumber1),
            Mono.just(blockNumber2),
        )
        every { ethereum.ethGetBlockByNumber(blockNumber1) } returns Mono.just(block1)
        every { ethereum.ethGetBlockByNumber(blockNumber2) } returns Mono.just(block2)

        val result = poller.newHeads().take(2).collectList().awaitFirst()
        assertThat(result.map { it.block }).containsExactly(block1, block2)
    }
}
