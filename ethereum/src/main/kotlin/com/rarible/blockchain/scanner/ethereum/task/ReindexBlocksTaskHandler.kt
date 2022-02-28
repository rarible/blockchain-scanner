package com.rarible.blockchain.scanner.ethereum.task

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.ethereum.handler.ReindexHandler
import com.rarible.blockchain.scanner.ethereum.handler.ReindexHandlerPlanner
import com.rarible.blockchain.scanner.ethereum.model.ReindexParam
import com.rarible.core.task.TaskHandler
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
@ExperimentalCoroutinesApi
class ReindexBlocksTaskHandler(
    private val reindexHandler: ReindexHandler,
    private val reindexHandlerPlanner: ReindexHandlerPlanner,
    private val blockchainScannerProperties: BlockchainScannerProperties
) : TaskHandler<Long> {

    override val type = "BLOCK_SCANNER_REINDEX_TASK"

    override suspend fun isAbleToRun(param: String): Boolean {
        return param.isNotBlank() && blockchainScannerProperties.scan.runReindexTask
    }

    override fun runLongTask(from: Long?, param: String): Flow<Long> {
        val taskParam = mapper.readValue(param, ReindexParam::class.java)

        val topics = taskParam.topics
        val addresses = taskParam.addresses

        return flow {
            val (reindexRanges, baseBlock) = reindexHandlerPlanner.getReindexPlan(taskParam, from)
            emitAll(reindexHandler.reindex(baseBlock, reindexRanges, topics, addresses))
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
