package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.util.flatten
import com.rarible.core.task.RunTask
import com.rarible.core.task.TaskHandler
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class ReconciliationTaskHandler(
    private val reconciliationExecutor: ReconciliationExecutor,
    private val properties: BlockchainScannerProperties
) : TaskHandler<Long> {

    private val logger = LoggerFactory.getLogger(ReconciliationTaskHandler::class.java)

    override val type: String
        get() = RECONCILIATION

    override fun getAutorunParams(): List<RunTask> {
        if (properties.job.reconciliation.enabled) {
            return reconciliationExecutor.getDescriptorIds().map {
                logger.info("Creating reconciliation task for descriptor with id '{}'", it)
                RunTask(it, null)
            }
        }
        logger.info("Reconciliation disabled, no tasks will be launched")
        return emptyList()
    }

    override fun runLongTask(from: Long?, descriptorId: String): Flow<Long> = flatten {
        reconciliationExecutor.reconcile(descriptorId, from ?: 1)
            .map { it.first }
    }

    companion object {
        const val RECONCILIATION = "RECONCILIATION"
    }
}