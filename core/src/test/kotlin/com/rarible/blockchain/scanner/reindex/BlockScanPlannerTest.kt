package com.rarible.blockchain.scanner.reindex

import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.configuration.BlockBatchLoadProperties
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.configuration.ScanProperties
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.configuration.TestBlockchainScannerProperties
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.data.randomBlock
import com.rarible.blockchain.scanner.test.data.randomBlockchain
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class BlockScanPlannerTest {

    private val lastBlockNumber = 100L
    private val startBlock = randomBlock(id = 20)
    private val prevBlock = randomBlock(id = startBlock.id - 1)
    private val batchLoad = BlockBatchLoadProperties(confirmationBlockDistance = 12, batchSize = 10)
    private val maxBlock = lastBlockNumber - batchLoad.confirmationBlockDistance

    private val blockService = mockk<BlockService>() {
        coEvery { getBlock(startBlock.id) } returns startBlock
        coEvery { getBlock(prevBlock.id) } returns prevBlock
    }

    private val blocks = randomBlockchain(lastBlockNumber.toInt())
    private val blockchainClient = TestBlockchainClient(TestBlockchainData(blocks))
    private val planner = BlockScanPlanner(blockService, blockchainClient, withBatchProperties(batchLoad))

    @Test
    fun `scan plan - ok`() = runBlocking<Unit> {
        val to = 50L
        val range = BlockRange(startBlock.id, to, 5)
        val plan = planner.getPlan(range)
        val batches = plan.ranges.toList()

        assertThat(plan.baseBlock).isEqualTo(prevBlock)
        assertThat(plan.from).isEqualTo(startBlock.id)
        assertThat(plan.to).isEqualTo(to)
        assertThat(batches).hasSize(7) // (50-20+1)/5

        val firstBatch = batches.first().range
        val lastBatch = batches.last().range

        assertThat(firstBatch.first).isEqualTo(startBlock.id)
        assertThat(firstBatch.last - firstBatch.first).isEqualTo(5 - 1)
        assertThat(lastBatch.last).isEqualTo(to)
        assertThat(lastBatch.last - lastBatch.first).isEqualTo(0) // last range contains only #50 block
    }

    @Test
    fun `scan plan - ok, without end block and batch size`() = runBlocking<Unit> {
        val range = BlockRange(startBlock.id, null, null)
        val plan = planner.getPlan(range)
        val batches = plan.ranges.toList()

        assertThat(plan.baseBlock).isEqualTo(prevBlock) // Base block is the block going right BEFORE the start block
        assertThat(plan.from).isEqualTo(startBlock.id)
        assertThat(plan.to).isEqualTo(maxBlock)
        assertThat(batches).hasSize(7) // (100-20-12)/10

        assertThat(batches.first().range.first).isEqualTo(startBlock.id)
        assertThat(batches.last().range.last).isEqualTo(maxBlock)
    }

    @Test
    fun `scan plan - ok, from state`() = runBlocking<Unit> {
        val range = BlockRange(startBlock.id, null, null)
        val intermediateBlock = randomBlock(id = 39)
        coEvery { blockService.getBlock(intermediateBlock.id) } returns intermediateBlock

        val plan = planner.getPlan(range, intermediateBlock.id)
        val batches = plan.ranges.toList()

        assertThat(plan.baseBlock).isEqualTo(intermediateBlock)
        assertThat(plan.from).isEqualTo(intermediateBlock.id + 1)
        assertThat(plan.to).isEqualTo(maxBlock)
        assertThat(batches).hasSize(5) // (100-50-12)/10

        assertThat(batches.first().range.first).isEqualTo(intermediateBlock.id + 1)
        assertThat(batches.last().range.last).isEqualTo(maxBlock)
    }

    @Test
    fun `scan plan - failed, block not found`() = runBlocking<Unit> {
        val missingBlock = 105L
        val range = BlockRange(missingBlock, null, null)
        coEvery { blockService.getBlock(missingBlock - 1) } returns null

        assertThrows<IllegalStateException> { planner.getPlan(range) }
    }

    @Test
    fun `scan plan - ok, from state, without block in db`() = runBlocking<Unit> {
        val range = BlockRange(startBlock.id, null, null)
        val intermediateBlockNumber = 49L
        coEvery { blockService.getBlock(intermediateBlockNumber) } returns null

        val plan = planner.getPlan(range, intermediateBlockNumber)
        val batches = plan.ranges.toList()

        assertThat(plan.baseBlock.id).isEqualTo(intermediateBlockNumber)
        assertThat(plan.from).isEqualTo(intermediateBlockNumber + 1)
        assertThat(plan.to).isEqualTo(maxBlock)
        assertThat(batches).hasSize(4) // (100-50-12)/10

        assertThat(batches.first().range.first).isEqualTo(intermediateBlockNumber + 1)
        assertThat(batches.last().range.last).isEqualTo(maxBlock)
    }

    private fun withBatchProperties(batchProperties: BlockBatchLoadProperties): BlockchainScannerProperties {
        val scan = ScanProperties(batchLoad = batchProperties)
        return TestBlockchainScannerProperties().copy(scan = scan)
    }

}