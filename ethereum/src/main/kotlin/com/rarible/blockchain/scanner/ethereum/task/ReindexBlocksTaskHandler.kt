package com.rarible.blockchain.scanner.ethereum.task

import com.rarible.blockchain.scanner.ethereum.handler.ReindexHandler
import com.rarible.core.task.TaskHandler
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.Flow
import org.springframework.stereotype.Component
import scalether.domain.Address

@Component
class ReindexBlocksTaskHandler(
    private val reindexHandler: ReindexHandler
) : TaskHandler<Long> {

    override val type = "BLOCK_SCANNER_REINDEX_TASK"

    override suspend fun isAbleToRun(param: String): Boolean {
        return param.isNotBlank()
    }

    // param pattern example: `blocks:1,2,4;from:1;topics:0x0000,0x000;addresses:0x00000,0x00000`
    // blocks - section  with list of single blocks to reindex
    // from - section to rescribe reindex from target block to current block (current block is getting from state repository or from blockchain)
    // topics - section with topics witch should be reindex
    // addresses - section with addresses witch should be reindex
    override fun runLongTask(from: Long?, param: String): Flow<Long> {
        val taskParam = TaskParam.parse(param)
        return if (taskParam.fromBlock != null) {
            val start = from ?: taskParam.fromBlock
            reindexHandler.reindexFrom(start, taskParam.topics, taskParam.addresses)
        } else {
            throw UnsupportedOperationException("Block list reindex to supported")
        }
    }

    data class TaskParam(
        val blocks: List<Long>,
        val fromBlock: Long?,
        val topics: List<Word>,
        val addresses: List<Address>
    ) {
        companion object {
            fun parse(value: String): TaskParam {
                val parts = value.split(";")
                return TaskParam(
                    blocks = run {
                        parts
                            .firstOrNull { it.startsWith(BLOCKS_PARAM) }
                            ?.let { parseSection(it, BLOCKS_PARAM, ) { param -> param.toLong() } }
                            ?: emptyList()
                    },
                    fromBlock = run {
                        parts
                            .firstOrNull { it.startsWith(FROM_BLOCK_PARAM) }
                            ?.let {
                                parseSection(it, BLOCKS_PARAM, ) { param -> param.toLong() }.firstOrNull()
                            }
                    },
                    topics = run {
                        parts
                            .firstOrNull { it.startsWith(TOPICS_PARAM) }
                            ?.let { parseSection(it, TOPICS_PARAM, ) { param -> Word.apply(param) } }
                            ?: emptyList()
                    },
                    addresses = run {
                        parts
                            .firstOrNull { it.startsWith(ADDRESSES_PARAM) }
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
        const val FROM_BLOCK_PARAM = "from"
        const val TOPICS_PARAM = "topics"
        const val ADDRESSES_PARAM = "addresses"
    }
}