package com.rarible.blockchain.scanner.ethereum.task

import com.rarible.blockchain.scanner.ethereum.handler.ReindexHandler
import com.rarible.core.task.TaskHandler
import kotlinx.coroutines.flow.Flow
import org.springframework.stereotype.Component

@Component
class ReindexBlocksTaskHandler(
    private val reindexHandler: ReindexHandler
) : TaskHandler<Long> {

    override val type = "BLOCK_SCANNER_REINDEX_TASK"

    override suspend fun isAbleToRun(param: String): Boolean {
        return param.isNotBlank()
    }

    override fun runLongTask(from: Long?, param: String): Flow<Long> {
        return reindexHandler.reindex(from ?: 0)
    }
}