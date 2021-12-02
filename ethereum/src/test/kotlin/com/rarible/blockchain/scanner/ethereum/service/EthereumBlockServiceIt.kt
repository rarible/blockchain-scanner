package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.test.AbstractIntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.IntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.data.randomBlock
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@FlowPreview
@ExperimentalCoroutinesApi
@IntegrationTest
class EthereumBlockServiceIt : AbstractIntegrationTest() {

    @Test
    fun `save and get`() = runBlocking {
        val block = randomBlock()

        ethereumBlockService.save(block)

        assertEquals(block, ethereumBlockService.getBlock(block.id))
    }

    @Test
    fun `get last block`() = runBlocking {
        val block1 = saveBlock(randomBlock())
        val block2 = saveBlock(randomBlock())
        val block3 = saveBlock(randomBlock())

        val max = listOf(block1, block2, block3).maxBy { it.id }!!.id

        val lastBlock = ethereumBlockService.getLastBlock()

        assertEquals(max, lastBlock?.id)
    }
}
