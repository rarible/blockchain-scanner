package com.rarible.blockchain.scanner.reconciliation

import com.rarible.core.task.TaskHandler
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import org.springframework.stereotype.Component

@Component
class ReconciliationTaskHandler(
    private val reconciliationExecutor: ReconciliationExecutor
) : TaskHandler<Long> {

    override val type: String
        get() = TOPIC

    override fun runLongTask(from: Long?, param: String): Flow<Long> {
        return reconciliationExecutor.reconcile(param, from ?: 1)
            .map { it.first }
            .asFlow()
    }

    companion object {
        const val TOPIC = "TOPIC"
    }
}