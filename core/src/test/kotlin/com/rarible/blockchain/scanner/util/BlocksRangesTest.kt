package com.rarible.blockchain.scanner.util

import com.rarible.blockchain.scanner.framework.data.NewStableBlockEvent
import com.rarible.blockchain.scanner.framework.data.NewUnstableBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.handler.BlocksRange
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.data.randomBlockHash
import com.rarible.blockchain.scanner.test.data.randomBlockchainBlock
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class BlocksRangesTest {

    @Test
    fun `get ranges`() {
        assertThat(range(1, 9, 10)).isEqualTo(listOf(1L..9L))
        assertThat(range(1, 10, 10)).isEqualTo(listOf(1L..10L))
        assertThat(range(1, 11, 10)).isEqualTo(listOf(1L..10L, 11L..11L))
        assertThat(range(5, 5, 10)).isEqualTo(listOf(5L..5L))
        assertThat(range(6, 5, 10)).isEqualTo(emptyList<Long>())
    }

    @Test
    fun `get stable unstable block ranges`() {
        assertThat(
            BlockRanges.getStableUnstableBlockRanges(
                lastKnownBlockNumber = 1,
                newBlockNumber = 5,
                batchSize = 10,
                stableDistance = 100
            ).toList()
        ).isEqualTo(
            listOf(
                BlocksRange(2..5L, false)
            )
        )

        assertThat(
            BlockRanges.getStableUnstableBlockRanges(
                lastKnownBlockNumber = 1,
                newBlockNumber = 10,
                batchSize = 100,
                stableDistance = 8
            ).toList()
        ).isEqualTo(
            listOf(
                BlocksRange(2..2L, true),
                BlocksRange(3..10L, false)
            )
        )

        assertThat(
            BlockRanges.getStableUnstableBlockRanges(
                lastKnownBlockNumber = 1,
                newBlockNumber = 10,
                batchSize = 4,
                stableDistance = 5
            ).toList()
        ).isEqualTo(
            listOf(
                BlocksRange(2..5L, true),
                BlocksRange(6..9L, false),
                BlocksRange(10..10L, false)
            )
        )
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
        val b1 = NewUnstableBlockEvent(
            randomBlockchainBlock(number = 1)
        )

        val batches = BlockRanges.toBatches(listOf(b1))

        assertThat(batches).hasSize(1)
        assertThat(batches[0]).hasSize(1)
    }

    @Test
    fun `to batches - one batch`() {
        val b1 = NewUnstableBlockEvent(
            randomBlockchainBlock(number = 1)
        )
        val b2 = NewUnstableBlockEvent(
            randomBlockchainBlock(number = 2)
        )

        val batches = BlockRanges.toBatches(listOf(b1, b2))

        assertThat(batches).hasSize(1)
        assertThat(batches[0]).hasSize(2)
        assertThat(batches[0][0]).isEqualTo(b1)
        assertThat(batches[0][1]).isEqualTo(b2)
    }

    @Test
    fun `to batches - mixed`() {
        val b0 = NewStableBlockEvent(randomBlockchainBlock(number = 9))
        val b1 = NewUnstableBlockEvent(randomBlockchainBlock(number = 10))
        val b2 = RevertedBlockEvent<TestBlockchainBlock>(number = 10, hash = randomBlockHash())
        val b3 = NewUnstableBlockEvent(randomBlockchainBlock(number = 10))
        val b4 = NewUnstableBlockEvent(randomBlockchainBlock(number = 11))

        val batches = BlockRanges.toBatches(listOf(b0, b1, b2, b3, b4))
        assertThat(batches).isEqualTo(
            listOf(
                listOf(b0),
                listOf(b1),
                listOf(b2),
                listOf(b3, b4)
            )
        )
    }

    private fun range(from: Long, to: Long, step: Int): List<LongRange> =
        BlockRanges.getRanges(from, to, step).toList()

}
