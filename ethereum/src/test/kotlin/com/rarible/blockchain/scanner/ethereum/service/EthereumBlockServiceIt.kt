package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.test.AbstractIntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.IntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.data.randomBlock
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

@FlowPreview
@ExperimentalCoroutinesApi
@IntegrationTest
class EthereumBlockServiceIt : AbstractIntegrationTest() {

    @Test
    fun `save and get`() = runBlocking<Unit> {
        val block = randomBlock()

        ethereumBlockService.save(block)

        assertThat(ethereumBlockService.getBlock(block.id)).isEqualTo(block)
    }

    @Test
    fun `get last block`() = runBlocking<Unit> {
        val block1 = saveBlock(randomBlock())
        val block2 = saveBlock(randomBlock())
        val block3 = saveBlock(randomBlock())

        val max = listOf(block1, block2, block3).maxByOrNull { it.id }!!.id

        val lastBlock = ethereumBlockService.getLastBlock()

        assertThat(lastBlock?.id).isEqualTo(max)
    }

    @Test
    fun `delete by id`() = runBlocking<Unit> {
        val block = randomBlock()

        ethereumBlockService.save(block)
        ethereumBlockService.remove(block.id)

        assertThat(ethereumBlockService.getBlock(block.id)).isNull()
    }
}
