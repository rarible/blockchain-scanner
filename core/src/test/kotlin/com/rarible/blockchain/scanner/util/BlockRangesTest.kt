package com.rarible.blockchain.scanner.util

import kotlinx.coroutines.flow.toCollection
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class BlockRangesTest {

    @Test
    fun `get ranges`() = runBlocking<Unit> {
        assertThat(range(1, 9, 10)).isEqualTo(listOf(1L..9L))
        assertThat(range(1, 10, 10)).isEqualTo(listOf(1L..10L))
        assertThat(range(1, 11, 10)).isEqualTo(listOf(1L..10L, 11L..11L))
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

    private suspend fun range(from: Long, to: Long, step: Int): List<LongRange> {
        return BlockRanges.getRanges(from, to, step).toCollection(mutableListOf())
    }


}
