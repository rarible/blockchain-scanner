package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.ethereum.configuration.BlockPollerProperties
import com.rarible.blockchain.scanner.framework.model.ReceivedBlock
import io.daonomic.rpc.domain.Word
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.runBlocking
import org.apache.commons.lang3.RandomUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import scalether.core.MonoEthereum
import scalether.domain.response.Block
import java.math.BigInteger
import java.time.Duration.ofSeconds

@Suppress("ReactiveStreamsUnusedPublisher")
internal class EthereumNewBlockPollerTest {
    private val ethereum = mockk<MonoEthereum>()

    @ExperimentalCoroutinesApi
    private val poller = EthereumNewBlockPoller(ethereum, BlockPollerProperties(period = ofSeconds(2)))

    @ExperimentalCoroutinesApi
    @Test
    fun polling() = runBlocking<Unit> {
        val blockNumber1 = BigInteger("1")
        val blockNumber2 = BigInteger("2")
        val block1 = mockk<Block<Word>> {
            every { number() } returns BigInteger.ONE
            every { hash() } returns Word(RandomUtils.nextBytes(32))
        }
        val block2 = mockk<Block<Word>> {
            every { number() } returns BigInteger.TEN
            every { hash() } returns Word(RandomUtils.nextBytes(32))
        }
        every { ethereum.ethBlockNumber() } returnsMany listOf(
            Mono.just(blockNumber1),
            Mono.just(blockNumber1),
            Mono.just(blockNumber2),
            Mono.just(blockNumber2),
        )
        every { ethereum.ethGetBlockByNumber(blockNumber1) } returns Mono.just(block1)
        every { ethereum.ethGetBlockByNumber(blockNumber2) } returns Mono.just(block2)

        val result = mutableListOf<ReceivedBlock<Block<Word>>>()
        poller.newHeads().take(2).collect { receivedBlock -> result.add(receivedBlock) }
        assertThat(result.map { it.block }).containsExactly(block1, block2)
    }
}
