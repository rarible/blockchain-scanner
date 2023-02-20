package com.rarible.blockchain.scanner.task

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rarible.blockchain.scanner.BlockchainScannerManager
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.reindex.BlockRange
import com.rarible.core.task.TaskHandler
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.slf4j.LoggerFactory

// Implement in blockchains if needed
abstract class BlockCheckTaskHandler<BB : BlockchainBlock, BL : BlockchainLog, R : LogRecord, D : Descriptor>(
    manager: BlockchainScannerManager<BB, BL, R, D>
) : TaskHandler<Long> {

    override val type = "BLOCK_SCANNER_CHECK_BLOCKS_HASH_CONSISTENCY_TASK"

    private val logger = LoggerFactory.getLogger(javaClass)
    private val mapper = ObjectMapper().registerModules().registerKotlinModule()

    private val checker = manager.blockChecker
    private val planner = manager.blockScanPlanner
    private val properties = manager.properties

    override suspend fun isAbleToRun(param: String): Boolean {
        return param.isNotBlank() && properties.task.checkBlocks.enabled
    }

    override fun runLongTask(from: Long?, param: String): Flow<Long> {
        val taskParam = mapper.readValue(param, BlockRange::class.java)

        return flow {
            val (checkRanges, _) = planner.getPlan(taskParam, from)
            emitAll(checker.check(checkRanges))
        }.map {
            logger.info("Check finished up to block $it")
            it.id
        }
    }
}
