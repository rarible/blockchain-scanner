package com.rarible.blockchain.scanner.util

import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.ReindexBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.test.data.randomBlockHash
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class BlockBatcherTest {

    @Test
    fun `to batches - single`() {
        val b1 = NewBlockEvent(
            number = 1,
            hash = randomBlockHash()
        )

        val batches = BlockBatcher.toBatches(listOf(b1))

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

        val batches = BlockBatcher.toBatches(listOf(b1, b2))

        assertThat(batches).hasSize(1)
        assertThat(batches[0]).hasSize(2)
        assertThat(batches[0][0]).isEqualTo(b1)
        assertThat(batches[0][1]).isEqualTo(b2)
    }

    @Test
    fun `to batches - mixed`() {
        val b1 = NewBlockEvent(
            number = 10,
            hash = randomBlockHash()
        )
        val b2 = RevertedBlockEvent(
            number = 9,
            hash = randomBlockHash()
        )
        val b3 = ReindexBlockEvent(number = 4)
        val b4 = ReindexBlockEvent(number = 5)
        val b5 = NewBlockEvent(
            number = 11,
            hash = randomBlockHash()
        )
        val b6 = NewBlockEvent(
            number = 12,
            hash = randomBlockHash()
        )

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
