package com.rarible.blockchain.scanner.block

import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.randomBlock
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

@IntegrationTest
class BlockServiceIt : AbstractIntegrationTest() {

    @Test
    fun `insert missing`() = runBlocking<Unit> {
        val block1 = randomBlock()
        val block2 = randomBlock()
        val block3 = randomBlock()

        blockService.insertAll(listOf(block1, block2))
        blockService.insertMissing(listOf(block1, block3))

        assertThat(blockService.getBlock(block1.id)).isEqualTo(block1)
        assertThat(blockService.getBlock(block2.id)).isEqualTo(block2)
        assertThat(blockService.getBlock(block3.id)).isEqualTo(block3)
    }
}
