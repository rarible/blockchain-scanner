package com.rarible.blockchain.scanner.util

import com.rarible.blockchain.scanner.test.data.randomNewBlockEvent
import com.rarible.blockchain.scanner.test.data.randomReindexBlockEvent
import com.rarible.blockchain.scanner.test.data.randomRevertedBlockEvent
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class BlockBatcherTest {

    @Test
    fun `to batches - single`() {
        val b1 = randomNewBlockEvent(1)

        val batches = BlockBatcher.toBatches(listOf(b1))

        assertThat(batches).hasSize(1)
        assertThat(batches[0]).hasSize(1)
    }

    @Test
    fun `to batches - one batch`() {
        val b1 = randomNewBlockEvent(1)
        val b2 = randomNewBlockEvent(2)

        val batches = BlockBatcher.toBatches(listOf(b1, b2))

        assertThat(batches).hasSize(1)
        assertThat(batches[0]).hasSize(2)
        assertThat(batches[0][0]).isEqualTo(b1)
        assertThat(batches[0][1]).isEqualTo(b2)
    }

    @Test
    fun `to batches - mixed`() {
        val b1 = randomNewBlockEvent(10)
        val b2 = randomRevertedBlockEvent(9)
        val b3 = randomReindexBlockEvent(4)
        val b4 = randomReindexBlockEvent(5)
        val b5 = randomNewBlockEvent(11)
        val b6 = randomNewBlockEvent(12)

        val batches = BlockBatcher.toBatches(listOf(b1, b2, b3, b4, b5, b6))

        assertThat(batches).hasSize(4)

        val batch1 = batches[0]
        val batch2 = batches[1]
        val batch3 = batches[2]
        val batch4 = batches[3]

        assertThat(batch1).hasSize(1)
        assertThat(batch1[0]).isEqualTo(b1)

        assertThat(batch2).hasSize(1)
        assertThat(batch2[0]).isEqualTo(b2)

        assertThat(batch3).hasSize(2)
        assertThat(batch3[0]).isEqualTo(b3)
        assertThat(batch3[1]).isEqualTo(b4)

        assertThat(batch4).hasSize(2)
        assertThat(batch4[0]).isEqualTo(b5)
        assertThat(batch4[1]).isEqualTo(b6)
    }


}