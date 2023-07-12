package com.rarible.blockchain.scanner.ethereum.reduce

import com.rarible.blockchain.scanner.ethereum.model.EthereumBlockStatus
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TestEventStatusReducer(
    forwardChainReducer: EntityChainReducer<String, ItemEvent, Item>,
    reversedChainReducer: RevertedEntityChainReducer<String, ItemEvent, Item>,
    revertCompactEventsReducer: RevertCompactEventsReducer<String, ItemEvent, Item>
) : EventStatusReducer<String, ItemEvent, Item>(forwardChainReducer, reversedChainReducer, revertCompactEventsReducer)

internal class EventStatusReducerTest {
    private val forwardChainItemReducer = mockk<EntityChainReducer<String, ItemEvent, Item>>()
    private val reversedChainItemReducer = mockk<RevertedEntityChainReducer<String, ItemEvent, Item>>()
    private val revertCompactEventsReducer = mockk<RevertCompactEventsReducer<String, ItemEvent, Item>> {
        coEvery { reduce(any(), any(), any()) } coAnswers  {
            val reducer = arg<RevertedEntityChainReducer<String, ItemEvent, Item>>(0)
            reducer.reduce(arg<Item>(1), arg<ItemEvent>(2))
        }
    }

    private val eventStatusItemReducer = TestEventStatusReducer(
        forwardChainReducer = forwardChainItemReducer,
        reversedChainReducer = reversedChainItemReducer,
        revertCompactEventsReducer = revertCompactEventsReducer,
    )

    @Test
    fun `should handle confirm event`() = runBlocking<Unit> {
        val event = createRandomItemEvent()
            .let { it.copy(log = it.log.copy(status = EthereumBlockStatus.CONFIRMED)) }
        val item = createRandomItem()

        coEvery { forwardChainItemReducer.reduce(item, event) } returns item
        val reducedItem = eventStatusItemReducer.reduce(item, event)
        assertThat(reducedItem).isEqualTo(item)

        coVerify { forwardChainItemReducer.reduce(item, event) }
        coVerify(exactly = 0) { reversedChainItemReducer.reduce(any(), any()) }
    }

    @Test
    fun `should handle revert event`() = runBlocking<Unit> {
        val event = createRandomItemEvent()
            .let { it.copy(log = it.log.copy(status = EthereumBlockStatus.REVERTED)) }
        val item = createRandomItem()

        coEvery { reversedChainItemReducer.reduce(item, event) } returns item
        val reducedItem = eventStatusItemReducer.reduce(item, event)
        assertThat(reducedItem).isEqualTo(item)

        coVerify { reversedChainItemReducer.reduce(item, event) }
        coVerify(exactly = 0) { forwardChainItemReducer.reduce(any(), any()) }
    }

    @Test
    fun `should handle pending event`() = runBlocking<Unit> {
        val event = createRandomItemEvent().let { it.copy(log = it.log.copy(status = EthereumBlockStatus.PENDING)) }
        val item = createRandomItem()

        val reducedItem = eventStatusItemReducer.reduce(item, event)
        assertThat(reducedItem).isEqualTo(item)

        coVerify(exactly = 0) { forwardChainItemReducer.reduce(any(), any()) }
        coVerify(exactly = 0) { reversedChainItemReducer.reduce(any(), any()) }
    }

    @Test
    fun `should handle inactive event`() = runBlocking<Unit> {
        val event = createRandomItemEvent()
            .let { it.copy(log = it.log.copy(status = EthereumBlockStatus.INACTIVE)) }
        val item = createRandomItem()

        val reducedItem = eventStatusItemReducer.reduce(item, event)
        assertThat(reducedItem).isEqualTo(item)

        coVerify(exactly = 0) { forwardChainItemReducer.reduce(any(), any()) }
        coVerify(exactly = 0) { reversedChainItemReducer.reduce(any(), any()) }
    }

    @Test
    fun `should handle drop event`() = runBlocking<Unit> {
        val event = createRandomItemEvent()
            .let { it.copy(log = it.log.copy(status = EthereumBlockStatus.DROPPED)) }
        val item = createRandomItem()

        val reducedItem = eventStatusItemReducer.reduce(item, event)
        assertThat(reducedItem).isEqualTo(item)

        coVerify(exactly = 0) { forwardChainItemReducer.reduce(any(), any()) }
        coVerify(exactly = 0) { reversedChainItemReducer.reduce(any(), any()) }
    }
}
