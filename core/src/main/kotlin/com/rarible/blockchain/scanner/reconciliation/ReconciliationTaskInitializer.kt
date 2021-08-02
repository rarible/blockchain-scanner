package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.core.task.TaskService
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@ExperimentalCoroutinesApi
@Component
class ReconciliationTaskInitializer(
    private val taskService: TaskService,
    private val reconciliationExecutor: ReconciliationExecutor,
    private val properties: BlockchainScannerProperties
) {
    //todo кажется это можно сделать с помощью autoRun в TaskHandler. я его позже добавил уже, чем этот initializer
    @Scheduled(initialDelayString = "\${taskInitializerDelay:60000}", fixedDelay = Long.MAX_VALUE)
    fun initialize() {
        if (properties.reindexEnabled) {
            reconciliationExecutor.getTopics()
                .forEach {
                    taskService.runTask(ReconciliationTaskHandler.TOPIC, it)
                }
        }
    }
}