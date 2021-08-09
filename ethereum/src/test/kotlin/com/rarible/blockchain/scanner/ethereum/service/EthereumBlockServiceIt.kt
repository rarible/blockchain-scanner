package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.test.AbstractIntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.IntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.data.randomBlock
import com.rarible.blockchain.scanner.framework.model.Block
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

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

        val lastBlock = ethereumBlockService.getLastBlockNumber()

        assertEquals(max, lastBlock)
    }

    @Test
    fun `update status`() = runBlocking {
        val block = saveBlock(randomBlock(), status = Block.Status.SUCCESS)

        ethereumBlockService.updateStatus(block.id, Block.Status.ERROR)

        assertEquals(Block.Status.ERROR, findBlock(block.id)!!.status)
    }

    @Test
    fun `find by status`() = runBlocking {
        val success1 = saveBlock(randomBlock(), status = Block.Status.SUCCESS)
        val success2 = saveBlock(randomBlock(), status = Block.Status.SUCCESS)
        saveBlock(randomBlock(), status = Block.Status.ERROR)
        saveBlock(randomBlock(), status = Block.Status.PENDING)

        val found = ethereumBlockService.findByStatus(Block.Status.SUCCESS).toList()

        assertEquals(2, found.size)
        assertEquals(success1, found[0])
        assertEquals(success2, found[1])
    }

}