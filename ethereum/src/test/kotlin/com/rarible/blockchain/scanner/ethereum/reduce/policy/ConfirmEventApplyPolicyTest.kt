package com.rarible.blockchain.scanner.ethereum.reduce.policy

import com.rarible.blockchain.scanner.ethereum.model.EthereumBlockStatus
import com.rarible.blockchain.scanner.ethereum.reduce.ItemEvent
import com.rarible.blockchain.scanner.ethereum.reduce.createRandomItemEvent
import com.rarible.blockchain.scanner.ethereum.reduce.withNewValues

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class ConfirmEventApplyPolicyTest {
    private val policy = ConfirmEventApplyPolicy<ItemEvent>(10)

    @Test
    fun `should say false on event if not any confirms events in list`() {
        val events = notConfirmStatuses().map { createRandomItemEvent().withNewValues(status = it) }
        val event = createRandomItemEvent().withNewValues(status = EthereumBlockStatus.CONFIRMED)
        assertThat(policy.wasApplied(events, event)).isFalse
    }

    @Test
    fun `should say false on event if confirm event is latest`() {
        val events = (1L..20).map { blockNumber ->
            createRandomItemEvent().withNewValues(
                status = EthereumBlockStatus.CONFIRMED,
                blockNumber = blockNumber
            )
        } + notConfirmStatuses().map { createRandomItemEvent().withNewValues(status = it) }

        val event = createRandomItemEvent().withNewValues(
            status = EthereumBlockStatus.CONFIRMED,
            blockNumber = 21
        )
        assertThat(policy.wasApplied(events, event)).isFalse()
    }

    @Test
    fun `should say true on event if confirm event is from past`() {
        val events = (10L..20).map { blockNumber ->
            createRandomItemEvent().withNewValues(
                status = EthereumBlockStatus.CONFIRMED,
                blockNumber = blockNumber
            )
        } + notConfirmStatuses().map { createRandomItemEvent().withNewValues(status = it) }

        val event1 = createRandomItemEvent().withNewValues(
            status = EthereumBlockStatus.CONFIRMED,
            blockNumber = 9
        )
        val event2 = createRandomItemEvent().withNewValues(
            status = EthereumBlockStatus.CONFIRMED,
            blockNumber = 1
        )
        assertThat(policy.wasApplied(events, event1)).isTrue()
        assertThat(policy.wasApplied(events, event2)).isTrue()
    }

    @Test
    fun `should add latest event to list`() {
        val events = (1L..5).map { blockNumber ->
            createRandomItemEvent().withNewValues(
                status = EthereumBlockStatus.CONFIRMED,
                blockNumber = blockNumber
            )
        } + notConfirmStatuses().map { createRandomItemEvent().withNewValues(status = it) }

        val event = createRandomItemEvent().withNewValues(
            status = EthereumBlockStatus.CONFIRMED,
            blockNumber = 6
        )
        val newEvents = policy.reduce(events, event)
        assertThat(newEvents).isEqualTo(newEvents)
    }

    @Test
    fun `should add latest event to list and remove not revertable events`() {
        val notRevertedEvent1 = createRandomItemEvent().withNewValues(
            status = EthereumBlockStatus.CONFIRMED,
            blockNumber = 1
        )
        val notRevertedEvent2 = createRandomItemEvent().withNewValues(
            status = EthereumBlockStatus.CONFIRMED,
            blockNumber = 2
        )
        val notRevertedEvent3 = createRandomItemEvent().withNewValues(
            status = EthereumBlockStatus.CONFIRMED,
            blockNumber = 3
        )
        val notConfirmEvent1 = createRandomItemEvent().withNewValues(status = EthereumBlockStatus.PENDING)
        val notConfirmEvent2 = createRandomItemEvent().withNewValues(status = EthereumBlockStatus.PENDING)
        val events = listOf(notRevertedEvent1, notRevertedEvent2, notRevertedEvent3, notConfirmEvent1, notConfirmEvent2)

        val confirmEvent = createRandomItemEvent().withNewValues(
            status = EthereumBlockStatus.CONFIRMED,
            blockNumber = 20
        )
        val newEvents = policy.reduce(events, confirmEvent)
        assertThat(newEvents).isEqualTo(listOf(notRevertedEvent3, notConfirmEvent1, notConfirmEvent2, confirmEvent))
    }

    private fun notConfirmStatuses(): List<EthereumBlockStatus> {
        return EthereumBlockStatus.values().filter { it != EthereumBlockStatus.CONFIRMED }
    }
}
