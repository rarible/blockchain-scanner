package com.rarible.blockchain.scanner.ethereum.task

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rarible.blockchain.scanner.ethereum.handler.HandlerPlanner
import com.rarible.blockchain.scanner.ethereum.handler.ReindexHandler
import com.rarible.blockchain.scanner.ethereum.model.ReindexParam
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.core.task.TaskHandler
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@ExperimentalCoroutinesApi
abstract class AbstractReindexBlocksTaskHandler(
    private val reindexHandler: ReindexHandler,
    private val reindexHandlerPlanner: HandlerPlanner,
    private val logRecordEventPublisher: LogRecordEventPublisher? = null
) : TaskHandler<Long> {

    protected val logger: Logger = LoggerFactory.getLogger(javaClass)

    override suspend fun isAbleToRun(param: String): Boolean {
        return param.isNotBlank()
    }

    override fun runLongTask(from: Long?, param: String): Flow<Long> {
        val taskParam = mapper.readValue(param, ReindexParam::class.java)

        val topics = taskParam.topics
        val addresses = taskParam.addresses

        return flow {
            val (reindexRanges, baseBlock) = reindexHandlerPlanner.getPlan(taskParam.range, from)
            val blocks = reindexHandler.reindex(
                baseBlock,
                reindexRanges,
                topics,
                addresses,
                logRecordEventPublisher
            )
            emitAll(blocks)
        }.map {
            logger.info("Re-index finished up to block $it")
            it.id
        }
    }

    protected companion object {
        val mapper = ObjectMapper().registerModules().registerKotlinModule()
    }
}