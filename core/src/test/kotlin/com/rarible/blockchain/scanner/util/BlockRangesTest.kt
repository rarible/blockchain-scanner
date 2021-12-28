package com.rarible.blockchain.scanner.util

import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.data.StableBlockEvent
import com.rarible.blockchain.scanner.test.data.randomBlockHash
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class BlockRangesTest {

    @Test
    fun `get ranges`() = runBlocking<Unit> {
        assertThat(range(1, 9, 10)).isEqualTo(listOf(1L..9L))
        assertThat(range(1, 10, 10)).isEqualTo(listOf(1L..10L))
        assertThat(range(1, 11, 10)).isEqualTo(listOf(1L..10L, 11L..11L))
        assertThat(range(5, 5, 10)).isEqualTo(listOf(5L..5L))
    }

    @Test
    fun `to ranges`() {
        val direct = BlockRanges.toRanges(listOf(1, 2, 3))
        assertThat(direct).hasSize(1)
        assertThat(LongRange(1, 3)).isEqualTo(direct[0])

        val mixed = BlockRanges.toRanges(listOf(3, 1, 2))
        assertThat(mixed).hasSize(2)
        assertThat(LongRange(3, 3)).isEqualTo(mixed[0])
        assertThat(LongRange(1, 2)).isEqualTo(mixed[1])

        val single = BlockRanges.toRanges(listOf(1))
        assertThat(single).hasSize(1)
        assertThat(LongRange(1, 1)).isEqualTo(single[0])

        val skipped = BlockRanges.toRanges(listOf(1, 2, 4))
        assertThat(skipped).hasSize(2)
        assertThat(LongRange(1, 2)).isEqualTo(skipped[0])
        assertThat(LongRange(4, 4)).isEqualTo(skipped[1])

        val empty = BlockRanges.toRanges(emptyList())
        assertThat(empty).hasSize(0)
    }

    @Test
    fun `to batches - single`() {
        val b1 = NewBlockEvent(
            number = 1,
            hash = randomBlockHash()
        )

        val batches = BlockRanges.toBatches(listOf(b1))

        assertThat(batches).hasSize(1)
        assertThat(batches[0]).hasSize(1)
    }

    @Test
    fun `to batches - one batch`() {
        val b1 = NewBlockEvent(
            number = 1,
            hash = randomBlockHash()
        )
        val b2 = NewBlockEvent(
            number = 2,
            hash = randomBlockHash()
        )

        val batches = BlockRanges.toBatches(listOf(b1, b2))

        assertThat(batches).hasSize(1)
        assertThat(batches[0]).hasSize(2)
        assertThat(batches[0][0]).isEqualTo(b1)
        assertThat(batches[0][1]).isEqualTo(b2)
    }

    @Test
    fun `to batches - mixed`() {
        val b0 = StableBlockEvent(number = 9, hash = randomBlockHash())
        val b1 = NewBlockEvent(number = 10, hash = randomBlockHash())
        val b2 = RevertedBlockEvent(number = 10, hash = randomBlockHash())
        val b3 = NewBlockEvent(number = 10, hash = randomBlockHash())
        val b4 = NewBlockEvent(number = 11, hash = randomBlockHash())

        val batches = BlockRanges.toBatches(listOf(b0, b1, b2, b3, b4))
        assertThat(batches).isEqualTo(listOf(
            listOf(b0),
            listOf(b1),
            listOf(b2),
            listOf(b3, b4)
        ))
    }

    private suspend fun range(from: Long, to: Long, step: Int): List<LongRange> =
        BlockRanges.getRanges(from, to, step).toList()

}
