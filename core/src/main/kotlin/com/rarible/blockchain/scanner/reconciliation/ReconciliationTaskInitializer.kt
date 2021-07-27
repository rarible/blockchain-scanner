package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.core.task.TaskService
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@ExperimentalCoroutinesApi
@Component
class ReconciliationTaskInitializer(
    private val taskService: TaskService,
    private val subscribers: List<LogEventSubscriber<*, *, *>>,
    @Value("\${ethereumBlockReindexEnabled:true}") private val reindexEnabled: Boolean
) {
    @Scheduled(initialDelayString = "\${taskInitializerDelay:60000}", fixedDelay = Long.MAX_VALUE)
    fun initialize() {
        if (reindexEnabled) {
            subscribers
                .map { it.getDescriptor().topic }
                .distinct()
                .forEach {
                    taskService.runTask(ReconciliationTaskHandler.TOPIC, it)
                }
        }
    }
}