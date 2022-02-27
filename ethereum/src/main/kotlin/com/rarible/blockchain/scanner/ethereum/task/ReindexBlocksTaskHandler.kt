package com.rarible.blockchain.scanner.ethereum.task

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.ethereum.handler.ReindexHandler
import com.rarible.core.task.TaskHandler
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.merge
import org.springframework.stereotype.Component
import scalether.domain.Address

@Component
@ExperimentalCoroutinesApi
class ReindexBlocksTaskHandler(
    private val reindexHandler: ReindexHandler,
    private val blockchainScannerProperties: BlockchainScannerProperties
) : TaskHandler<Long> {

    override val type = "BLOCK_SCANNER_REINDEX_TASK"

    override suspend fun isAbleToRun(param: String): Boolean {
        return param.isNotBlank() && blockchainScannerProperties.scan.runReindexTask
    }

    override fun runLongTask(from: Long?, param: String): Flow<Long> {
        val taskParam = mapper.readValue(param, TaskParam::class.java)

        val topics = taskParam.topics
        val addresses = taskParam.addresses

        return if (taskParam.blocks != null && taskParam.blocks.isNotEmpty()) {
            val blocks = taskParam.blocks.sorted().filter { block -> block > (from ?: -1) }
            flow {
                emitAll(
                    blocks.map { block ->
                        reindexHandler.reindexRange(LongRange(block, block), topics, addresses)
                    }.merge()
                )
            }
        } else if (taskParam.range?.from != null && taskParam.range.to != null) {
            val range = LongRange(from ?: taskParam.range.from, taskParam.range.to)
            flow {
                emitAll(reindexHandler.reindexRange(range, topics, addresses))
            }
        } else if (taskParam.range?.from != null) {
            flow {
                emitAll(reindexHandler.reindexFrom(from ?: taskParam.range.from, topics, addresses))
            }
        } else {
            throw IllegalArgumentException("Invalid param to run task (param=$param)")
        }
    }

    data class TaskParam(
        val range: BlockRange?,
        val blocks: List<Long>?,
        val topics: List<Word>,
        val addresses: List<Address>
    ) {
        data class BlockRange(
            val from: Long,
            val to: Long?
        )
    }

    private companion object {
        val mapper = ObjectMapper().registerModules().registerKotlinModule()
    }
}
