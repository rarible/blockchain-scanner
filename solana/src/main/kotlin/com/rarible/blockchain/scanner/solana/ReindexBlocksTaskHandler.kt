package com.rarible.blockchain.scanner.solana

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rarible.blockchain.scanner.solana.handler.ReindexHandler
import com.rarible.blockchain.scanner.solana.model.ReindexParam
import com.rarible.core.task.TaskHandler
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class ReindexBlocksTaskHandler(
    private val reindexHandler: ReindexHandler
) : TaskHandler<Long> {
    override val type: String = "SOLANA_BLOCK_SCANNER_REINDEX_TASK"

    override suspend fun isAbleToRun(param: String): Boolean = param.isNotBlank()

    override fun runLongTask(from: Long?, param: String): Flow<Long> {
        val taskParam = mapper.readValue(param, ReindexParam::class.java)

        return flow {
            emitAll(reindexHandler.reindex(taskParam.blocks))
        }.map {
            logger.info("Re-index finished up to block $it")
            it.id
        }
    }

    private companion object {
        val logger: Logger = LoggerFactory.getLogger(ReindexBlocksTaskHandler::class.java)
        val mapper = ObjectMapper().registerModules().registerKotlinModule()
    }
}