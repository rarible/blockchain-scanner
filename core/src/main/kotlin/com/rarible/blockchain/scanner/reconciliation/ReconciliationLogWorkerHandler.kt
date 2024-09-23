package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.block.BlockRepository
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.handler.TypedBlockRange
import com.rarible.core.daemon.job.JobHandler
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class ReconciliationLogWorkerHandler(
    private val reconciliationLogHandler: ReconciliationLogHandler,
    private val stateRepository: ReconciliationStateRepository,
    private val blockRepository: BlockRepository,
    private val scannerProperties: BlockchainScannerProperties,
    private val monitor: LogReconciliationMonitor,
) : JobHandler {

    override suspend fun handle() {
        val latestBlock = blockRepository.getLastBlock() ?: return
        val state = getState(latestBlock.id)
        val from = state.lastReconciledBlock + 1
        val to = latestBlock.id - scannerProperties.reconciliation.blockLag
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
