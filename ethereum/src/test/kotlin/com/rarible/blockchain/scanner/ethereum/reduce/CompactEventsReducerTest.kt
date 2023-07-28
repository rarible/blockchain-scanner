package com.rarible.blockchain.scanner.ethereum.reduce

import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CompactEventsReducerTest {
    private val reducer = object : CompactEventsReducer<String, ItemEvent, Item>() {

        override fun compact(events: List<ItemEvent>): List<ItemEvent> {
            val total = events.sumOf { it.supply }
            return listOf(events.last().copy(supply = total))
        }
    }

    @Test
    fun `reduce events`() = runBlocking<Unit> {
        val events = listOf(
            createRandomItemEvent(supply = 1, blockNumber = 1),
            createRandomItemEvent(supply = 1, blockNumber = 1),

            createRandomItemEvent(supply = 2, blockNumber = 2),
            createRandomItemEvent(supply = 2, blockNumber = 2),

            createRandomItemEvent(supply = 3, blockNumber = 3),
        )
        val item = createRandomItem().withRevertableEvents(events)

        val reduced = reducer.reduce(item, createRandomItemEvent())
        assertThat(reduced.revertableEvents).hasSize(3)

        assertThat(reduced.revertableEvents[0].supply).isEqualTo(2)
        assertThat(reduced.revertableEvents[0].log.blockNumber).isEqualTo(1)

        assertThat(reduced.revertableEvents[1].supply).isEqualTo(4)
        assertThat(reduced.revertableEvents[1].log.blockNumber).isEqualTo(2)

        assertThat(reduced.revertableEvents[2].supply).isEqualTo(3)
        assertThat(reduced.revertableEvents[2].log.blockNumber).isEqualTo(3)

        val reReduced = reducer.reduce(reduced, createRandomItemEvent())
        assertThat(reReduced.revertableEvents).isEqualTo(reduced.revertableEvents)
    }
}
