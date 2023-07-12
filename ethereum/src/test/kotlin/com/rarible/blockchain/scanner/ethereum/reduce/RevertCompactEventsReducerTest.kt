package com.rarible.blockchain.scanner.ethereum.reduce

import com.rarible.blockchain.scanner.ethereum.model.EthereumBlockStatus
import com.rarible.core.entity.reducer.service.Reducer
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class RevertCompactEventsReducerTest {
    private val delegate = mockk<Reducer<ItemEvent, Item>>()

    private val reducer = object : RevertCompactEventsReducer<String, ItemEvent, Item>() {
        override fun merge(reverted: ItemEvent, compact: ItemEvent): ItemEvent {
            return reverted.copy(supply = compact.supply)
        }
    }

    @Test
    fun `find compact and revert`() = runBlocking<Unit> {
        val log = createRandomEthereumLog(
            blockNumber = 1,
            logIndex = 1,
            minorLogIndex = 1,
            status = EthereumBlockStatus.CONFIRMED
        )
        val reverted = createRandomItemEvent(supply = 1).copy(log = log)
        val compact = createRandomItemEvent(supply = 2, compact = true).copy(log = log)

        val events = listOf(
            createRandomItemEvent(),
            compact,
            createRandomItemEvent(),
        )
        val item = createRandomItem().withRevertableEvents(events)

        coEvery { delegate.reduce(item, any()) } returns item

        reducer.reduce(delegate, item, reverted)

        coVerify {
            delegate.reduce(item, withArg {
                assertThat(it.supply).isEqualTo(2)
            })
        }
    }
}