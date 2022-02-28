package com.rarible.blockchain.scanner.ethereum.task

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.handler.CheckHandler
import com.rarible.blockchain.scanner.ethereum.handler.HandlerPlanner
import com.rarible.blockchain.scanner.ethereum.model.BlockRange
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
class CheckBlocksHashConsistencyTask(
    private val checkHandler: CheckHandler,
    private val reindexHandlerPlanner: HandlerPlanner,
    private val blockchainScannerProperties: EthereumScannerProperties
) : TaskHandler<Long> {

    override val type = "BLOCK_SCANNER_CHECK_BLOCKS_HASH_CONSISTENCY_TASK"

    override suspend fun isAbleToRun(param: String): Boolean {
        return param.isNotBlank() && blockchainScannerProperties.task.checkBlocks.enabled
    }

    override fun runLongTask(from: Long?, param: String): Flow<Long> {
        val taskParam = mapper.readValue(param, BlockRange::class.java)

        return flow {
            val (checkRanges, _) = reindexHandlerPlanner.getPlan(taskParam, from)
            emitAll(checkHandler.check(checkRanges))
        }.map {
            logger.info("Check finished up to block $it")
            it.id
        }
    }

    private companion object {
        val logger: Logger = LoggerFactory.getLogger(CheckBlocksHashConsistencyTask::class.java)
        val mapper = ObjectMapper().registerModules().registerKotlinModule()
    }
}
