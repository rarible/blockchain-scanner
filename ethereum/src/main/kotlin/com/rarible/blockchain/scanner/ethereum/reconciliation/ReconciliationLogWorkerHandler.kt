package com.rarible.blockchain.scanner.ethereum.reconciliation

import com.rarible.blockchain.scanner.block.BlockRepository
import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.model.ReconciliationLogState
import com.rarible.blockchain.scanner.ethereum.repository.EthereumReconciliationStateRepository
import com.rarible.blockchain.scanner.handler.TypedBlockRange
import com.rarible.core.daemon.job.JobHandler
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
@ExperimentalCoroutinesApi
class ReconciliationLogWorkerHandler(
    private val reconciliationLogHandler: ReconciliationLogHandler,
    private val stateRepository: EthereumReconciliationStateRepository,
    private val blockRepository: BlockRepository,
    private val scannerProperties: EthereumScannerProperties,
    private val monitor: EthereumLogReconciliationMonitor,
) : JobHandler {

    override suspend fun handle() {
        val latestBlock = blockRepository.getLastBlock() ?: return
        val state = getState(latestBlock.id)
        val from = state.lastReconciledBlock + 1
        val to = latestBlock.id - scannerProperties.scan.batchLoad.confirmationBlockDistance
        if (from >= to) return

        val range = TypedBlockRange(LongRange(from, to), stable = true)
        logger.info("Next reconcile block range {}", range)

        val lastReconciledBlock = reconciliationLogHandler.check(range, scannerProperties.reconciliation.batchSize)
        val newState = state.copy(lastReconciledBlock = lastReconciledBlock)
        stateRepository.saveReconciliationLogState(newState)
        monitor.onReconciledRange(size = to - from)
    }

    private suspend fun getState(latestBlock: Long): ReconciliationLogState {
        val state = stateRepository.getReconciliationLogState()
        return if (state == null) {
            val initState = ReconciliationLogState(latestBlock)
            logger.info("No init reconciliation, save for {} block", initState.lastReconciledBlock)
            stateRepository.saveReconciliationLogState(initState)
        } else state
    }

    private companion object {
        val logger: Logger = LoggerFactory.getLogger(ReconciliationLogWorkerHandler::class.java)
    }
}
