package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.util.flatten
import com.rarible.core.task.RunTask
import com.rarible.core.task.TaskHandler
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.stereotype.Component

@Component
@ConditionalOnBean(ReconciliationExecutor::class)
class ReconciliationTaskHandler(
    private val reconciliationExecutor: ReconciliationExecutor,
    private val properties: BlockchainScannerProperties,
    private val fromProvider: ReconciliationFromProvider
) : TaskHandler<Long> {

    private val jobProperties = properties.job.reconciliation

    private val logger = LoggerFactory.getLogger(ReconciliationTaskHandler::class.java)

    override val type: String
        get() = RECONCILIATION

    override fun getAutorunParams(): List<RunTask> {
        if (jobProperties.enabled) {
            return reconciliationExecutor.getDescriptorIds().map {
                logger.info("Creating reconciliation task for descriptor with id '{}'", it)
                RunTask(it, null)
            }
        }
        logger.info("Reconciliation disabled, no tasks will be launched")
        return emptyList()
    }

    override fun runLongTask(from: Long?, descriptorId: String): Flow<Long> = flatten {
        reconciliationExecutor.reconcile(descriptorId, from ?: fromProvider.initialFrom(), jobProperties.batchSize)
            .map { it.first }
    }

    companion object {
        const val RECONCILIATION = "RECONCILIATION"
    }
}
