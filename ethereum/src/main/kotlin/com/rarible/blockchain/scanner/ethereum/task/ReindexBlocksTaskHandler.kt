package com.rarible.blockchain.scanner.ethereum.task

import com.rarible.blockchain.scanner.ethereum.handler.ReindexHandler
import com.rarible.core.task.TaskHandler
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.merge
import org.springframework.stereotype.Component
import scalether.domain.Address
import java.util.regex.Pattern

@Component
class ReindexBlocksTaskHandler(
    private val reindexHandler: ReindexHandler
) : TaskHandler<Long> {

    override val type = "BLOCK_SCANNER_REINDEX_TASK"

    override suspend fun isAbleToRun(param: String): Boolean {
        return param.isNotBlank()
    }

    // param pattern example: `blocks:1,2,4;topics:0x0000,0x000;addresses:0x00000,0x00000`
    // blocks - section  with list of single blocks to reindex
    // from - section to rescribe reindex from target block to current block (current block is getting from state repository or from blockchain)
    // topics - section with topics witch should be reindex
    // addresses - section with addresses witch should be reindex
    override fun runLongTask(from: Long?, param: String): Flow<Long> {
        val taskParam = TaskParam.parse(param)
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
        } else if (taskParam.range != null) {
            val range = LongRange(from ?: taskParam.range.first, taskParam.range.last)
            flow {
                emitAll(
                    reindexHandler.reindexRange(range, topics, addresses)
                )
            }
        } else if (taskParam.from != null) {
            flow {
                emitAll(
                    reindexHandler.reindexFrom(from ?: taskParam.from, topics, addresses)
                )
            }
        } else {
            throw IllegalArgumentException("Invalid param to run task (param=$param)")
        }
    }

    data class TaskParam(
        val blocks: List<Long>?,
        val range: LongRange?,
        val from: Long?,
        val topics: List<Word>,
        val addresses: List<Address>
    ) {
        companion object {
            fun parse(value: String): TaskParam {
                val parts = value.split(";")
                val blocks = parts
                    .firstOrNull { it.startsWith(BLOCKS_PARAM) }
                    ?.split(":")
                    ?.let { BLOCK_PATTERN.matcher(it.getOrNull(1) ?: "") }
                    ?.takeIf { it.matches() }
                val topics = parts
                    .firstOrNull { it.startsWith(TOPICS_PARAM) }
                val addresses = parts
                    .firstOrNull { it.startsWith(ADDRESSES_PARAM) }

                return TaskParam(
                    blocks = run {
                        blocks
                            ?.group("blockList")
                            ?.takeIf { it.isNotBlank() }
                            ?.let { it.split(",").filter { block -> block.isNotBlank() }.map { block -> block.toLong() } }
                    },
                    range = run {
                        val rangeFrom = blocks?.group("rangeFrom")
                        val rangeTo = blocks?.group("rangeTo")
                        if (rangeFrom?.isNotBlank() == true && rangeTo?.isNotBlank() == true) LongRange(rangeFrom.toLong(), rangeTo.toLong()) else null
                    },
                    from = run {
                        blocks?.group("rangeFrom")?.takeIf { it.isNotBlank() }?.let { it.toLong() }
                    },
                    topics = run {
                        topics
                            ?.let { parseSection(it, TOPICS_PARAM, ) { param -> Word.apply(param) } }
                            ?: emptyList()
                    },
                    addresses = run {
                        addresses
                            ?.let { parseSection(it, ADDRESSES_PARAM, ) { param -> Address.apply(param) } }
                            ?: emptyList()
                    }
                )
            }

            private fun <T> parseSection(value: String, name: String, mapper: (String) -> T): List<T> {
                val blockSection = value.split(":")
                assert(blockSection.size == 2) { "Invalid $name param section: $value" }
                val blockParams = blockSection[1].split(",")
                assert(blockParams.isNotEmpty()) { "No values for $name param section" }
                return blockParams.map(mapper)
            }
        }
    }

    companion object {
        const val BLOCKS_PARAM = "blocks"
        const val TOPICS_PARAM = "topics"
        const val ADDRESSES_PARAM = "addresses"
        val BLOCK_PATTERN: Pattern = Pattern.compile("((?<rangeFrom>\\d+)-(?<rangeTo>\\d+)?)|(?<blockList>[\\d+,?]+)")
    }
}
