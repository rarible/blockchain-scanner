package com.rarible.blockchain.scanner.pending

import com.rarible.blockchain.scanner.BlockListener
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.data.randomOriginalBlock
import com.rarible.blockchain.scanner.test.model.TestBlock
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

@ExperimentalCoroutinesApi
@FlowPreview
@IntegrationTest
class DefaultPendingBlockCheckerIt : AbstractIntegrationTest() {

    private var blockListener: BlockListener = mockk()

    @BeforeEach
    fun beforeEach() {
        clearMocks(blockListener)
        coEvery { blockListener.onBlockEvent(any()) } returns Unit
    }

    @Test
    fun `check pending blocks`() = runBlocking {
        val oldBlockTimestamp = Instant.now().minusSeconds(70).epochSecond
        val newBlockTimestamp = Instant.now().epochSecond

        val processedBlock = saveBlock(randomOriginalBlock(1))

        // Yong Pending block should not be processed here, failed blocks should be processed anyway
        val pendingOldBlock = randomOriginalBlock(2).copy(timestamp = oldBlockTimestamp)
        val pendingNewBlock = randomOriginalBlock(3).copy(timestamp = newBlockTimestamp)
        val failedOldBlock = randomOriginalBlock(4).copy(timestamp = oldBlockTimestamp)
        val failedNewBlock = randomOriginalBlock(5).copy(timestamp = newBlockTimestamp)
        saveBlock(pendingOldBlock, Block.Status.PENDING)
        saveBlock(pendingNewBlock, Block.Status.PENDING)
        saveBlock(failedOldBlock, Block.Status.ERROR)
        saveBlock(failedNewBlock, Block.Status.ERROR)

        val testBlockchainData = TestBlockchainData(
            listOf(pendingOldBlock, pendingNewBlock, failedOldBlock, failedNewBlock, processedBlock),
            listOf()
        )

        val checker = createPendingBlockChecker(TestBlockchainClient(testBlockchainData))
        checker.checkPendingBlocks(Duration.ofMinutes(1))

        // 3 events should be emitted in total
        coVerify(exactly = 3) { blockListener.onBlockEvent(any()) }
        coVerify(exactly = 1) { blockListener.onBlockEvent(newBlockEvent(pendingOldBlock, Source.PENDING)) }
        coVerify(exactly = 1) { blockListener.onBlockEvent(newBlockEvent(failedOldBlock, Source.PENDING)) }
        coVerify(exactly = 1) { blockListener.onBlockEvent(newBlockEvent(failedNewBlock, Source.PENDING)) }
    }

    private fun createPendingBlockChecker(
        testBlockchainClient: TestBlockchainClient
    ): DefaultPendingBlockChecker<TestBlockchainBlock, TestBlockchainLog, TestBlock, TestDescriptor> {

        return DefaultPendingBlockChecker(
            testBlockchainClient,
            testBlockService,
            blockListener
        )
    }
}
