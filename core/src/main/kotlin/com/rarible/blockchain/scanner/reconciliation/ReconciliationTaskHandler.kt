package com.rarible.blockchain.scanner.reconciliation

import com.rarible.core.task.TaskHandler
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.springframework.stereotype.Component

@Component
class ReconciliationTaskHandler(
    private val reconciliationExecutor: ReconciliationExecutor
) : TaskHandler<Long> {

    override val type: String
        get() = TOPIC

    override fun runLongTask(from: Long?, param: String): Flow<Long> = runBlocking {
        reconciliationExecutor.reconcile(param, from ?: 1)
            .map { it.first }
    }

    companion object {
        const val TOPIC = "TOPIC"
    }
}