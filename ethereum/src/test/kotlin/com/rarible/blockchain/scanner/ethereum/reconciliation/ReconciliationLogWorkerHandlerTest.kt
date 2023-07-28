package com.rarible.blockchain.scanner.ethereum.reconciliation

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.block.BlockRepository
import com.rarible.blockchain.scanner.configuration.BlockBatchLoadProperties
import com.rarible.blockchain.scanner.configuration.ScanProperties
import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.configuration.ReconciliationProperties
import com.rarible.blockchain.scanner.ethereum.model.ReconciliationLogState
import com.rarible.blockchain.scanner.ethereum.repository.EthereumReconciliationStateRepository
import com.rarible.blockchain.scanner.handler.TypedBlockRange
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

@ExperimentalCoroutinesApi
internal class ReconciliationLogWorkerHandlerTest {
    private val reconciliationLogHandler = mockk<ReconciliationLogHandler>()
    private val stateRepository = mockk<EthereumReconciliationStateRepository>()
    private val blockRepository = mockk<BlockRepository>()
    private val monitor = mockk<EthereumLogReconciliationMonitor> {
        every { onReconciledRange(any()) } returns Unit
    }
    private val reconciliationProperties = mockk<ReconciliationProperties>() {
        every { batchSize } returns 10
    }
    private val blockBatchLoadProperties = mockk<BlockBatchLoadProperties> {
        every { confirmationBlockDistance } returns 10
    }
    private val scanProperties = mockk<ScanProperties> {
        every { batchLoad } returns blockBatchLoadProperties
    }
    private val scannerProperties = mockk<EthereumScannerProperties> {
        every { scan } returns scanProperties
        every { reconciliation } returns reconciliationProperties
    }
    private val handler = ReconciliationLogWorkerHandler(
        reconciliationLogHandler = reconciliationLogHandler,
        stateRepository = stateRepository,
        blockRepository = blockRepository,
        scannerProperties = scannerProperties,
        monitor = monitor
    )

    @Test
    fun `no handle - with null init state`() = runBlocking<Unit> {
        val latestBlock = mockk<Block> {
            every { id } returns 100
        }
        coEvery { blockRepository.getLastBlock() } returns latestBlock
        coEvery { stateRepository.getReconciliationLogState() } returns null
        coEvery { stateRepository.saveReconciliationLogState(any()) } answers { it.invocation.args.first() as ReconciliationLogState }

        handler.handle()

        coVerify(exactly = 1) { stateRepository.saveReconciliationLogState(withArg {
            assertThat(it.lastReconciledBlock).isEqualTo(100)
        }) }
        coVerify(exactly = 0) {
            reconciliationLogHandler.check(any(), any())
        }
    }

    @Test
    fun `no handle - if not enough block range`() = runBlocking<Unit> {
        val latestBlock = mockk<Block> {
            every { id } returns 100
        }
        coEvery { blockRepository.getLastBlock() } returns latestBlock
        coEvery { stateRepository.getReconciliationLogState() } returns ReconciliationLogState(91)

        handler.handle()

        coVerify(exactly = 0) { stateRepository.saveReconciliationLogState(any()) }
        coVerify(exactly = 0) { reconciliationLogHandler.check(any(), any()) }
    }

    @Test
    fun `handle - ok`() = runBlocking<Unit> {
        val latestBlock = mockk<Block> {
            every { id } returns 100
        }
        val savedState = ReconciliationLogState(70)
        val expectedRange = TypedBlockRange(LongRange(71, 90), true)
        val expectedNewState = ReconciliationLogState(90)

        coEvery { blockRepository.getLastBlock() } returns latestBlock
        coEvery { stateRepository.getReconciliationLogState() } returns savedState
        coEvery { reconciliationLogHandler.check(expectedRange, any()) } returns 90
        coEvery { stateRepository.saveReconciliationLogState(expectedNewState) } returns expectedNewState

        handler.handle()
    }
}
