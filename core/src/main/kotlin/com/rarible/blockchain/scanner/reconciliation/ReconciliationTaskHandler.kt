package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.util.flatten
import com.rarible.core.task.TaskHandler
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.springframework.stereotype.Component

@Component
class ReconciliationTaskHandler(
    private val reconciliationExecutor: ReconciliationExecutor
) : TaskHandler<Long> {

    override val type: String
        get() = RECONCILIATION

    override fun runLongTask(from: Long?, descriptorId: String): Flow<Long> = flatten {
        reconciliationExecutor.reconcile(descriptorId, from ?: 1)
            .map { it.first }
    }

    companion object {
        const val RECONCILIATION = "RECONCILIATION"
    }
}