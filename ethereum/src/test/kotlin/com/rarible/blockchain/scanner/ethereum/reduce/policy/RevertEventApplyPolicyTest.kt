package com.rarible.blockchain.scanner.ethereum.reduce.policy

import com.rarible.blockchain.scanner.ethereum.model.EthereumBlockStatus
import com.rarible.blockchain.scanner.ethereum.reduce.ItemEvent
import com.rarible.blockchain.scanner.ethereum.reduce.createRandomItemEvent
import com.rarible.blockchain.scanner.ethereum.reduce.withNewValues
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

internal class RevertEventApplyPolicyTest {
    private val revertEventApplyPolicy = RevertEventApplyPolicy<ItemEvent>()

    @Test
    fun `should remove confirm event`() {
        val mint = createRandomItemEvent().withNewValues(
            status = EthereumBlockStatus.CONFIRMED,
            blockNumber = 4,
            logIndex = 1,
            minorLogIndex = 1,
        )
        val revertedMint = mint.withNewValues(
            status = EthereumBlockStatus.REVERTED,
            blockNumber = 4,
            logIndex = 1,
            minorLogIndex = 1,
        )
        val events = (1L..3).map {
            createRandomItemEvent().withNewValues(
                status = EthereumBlockStatus.CONFIRMED,
                blockNumber = it,
                logIndex = 1,
                minorLogIndex = 1,
            )
        }
        val wasApplied = revertEventApplyPolicy.wasApplied(events + mint, revertedMint)
        assertThat(wasApplied).isTrue

        val reduced = revertEventApplyPolicy.reduce(events + mint, revertedMint)
        assertThat(reduced).isEqualTo(events)
    }

    @Test
    // TODO: back after bug in blockchain scanner wiil be fixed
    @Disabled
    fun `should throw exception if event not from tail`() {
        val mint = createRandomItemEvent().withNewValues(
            status = EthereumBlockStatus.CONFIRMED,
            blockNumber = 4,
            logIndex = 1,
            minorLogIndex = 1,
        )
        val burn = createRandomItemEvent().withNewValues(
            status = EthereumBlockStatus.CONFIRMED,
            blockNumber = 5,
            logIndex = 1,
            minorLogIndex = 1,
        )
        val revertedMint = mint.withNewValues(
            status = EthereumBlockStatus.REVERTED,
            blockNumber = 4,
            logIndex = 1,
            minorLogIndex = 1,
        )
        assertThrows<Exception> {
            revertEventApplyPolicy.reduce(listOf(mint, burn), revertedMint)
        }
    }

    @Test
    fun `should throw exception if event list is empty`() {
        val mint = createRandomItemEvent().withNewValues(
            status = EthereumBlockStatus.CONFIRMED,
            blockNumber = 4,
            logIndex = 1,
            minorLogIndex = 1,
        )
        assertThrows<Exception> {
            revertEventApplyPolicy.reduce(emptyList(), mint)
        }
    }

    @Test
    fun `should throw exception if try to revert too old event`() {
        val burn = createRandomItemEvent().withNewValues(
            status = EthereumBlockStatus.CONFIRMED,
            blockNumber = 5,
            logIndex = 1,
            minorLogIndex = 1,
        )
        val revertMint = createRandomItemEvent().withNewValues(
            status = EthereumBlockStatus.REVERTED,
            blockNumber = 4,
            logIndex = 1,
            minorLogIndex = 1,
        )
        assertThrows<Exception> {
            revertEventApplyPolicy.reduce(listOf(burn), revertMint)
        }
    }

    @Test
    fun `should say no if event was not applied`() {
        val mint = createRandomItemEvent().withNewValues(
            status = EthereumBlockStatus.CONFIRMED,
            blockNumber = 4,
            logIndex = 1,
            minorLogIndex = 1,
        )
        val revertedMint = mint.withNewValues(
            status = EthereumBlockStatus.REVERTED,
            blockNumber = 4,
            logIndex = 1,
            minorLogIndex = 1,
        )
        val events = (1L..3).map {
            createRandomItemEvent().withNewValues(
                status = EthereumBlockStatus.CONFIRMED,
                blockNumber = it,
                logIndex = 1,
                minorLogIndex = 1,
            )
        }
        val wasApplied = revertEventApplyPolicy.wasApplied(events + mint, revertedMint)
        assertThat(wasApplied).isTrue

        val reduced = revertEventApplyPolicy.reduce(events + mint, revertedMint)
        assertThat(reduced).isEqualTo(events)
    }
}
