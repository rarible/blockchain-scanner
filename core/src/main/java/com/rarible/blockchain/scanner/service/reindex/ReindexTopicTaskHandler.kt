package com.rarible.blockchain.scanner.service.reindex

import com.rarible.core.task.TaskHandler
import kotlinx.coroutines.flow.Flow
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import scalether.core.MonoEthereum

@Component
class ReindexTopicTaskHandler(
    private val blockchainListenerService: BlockIndexerService<*, *, *, *>,
    private val ethereum: MonoEthereum
) : TaskHandler<Long> {

    override val type: String
        get() = TOPIC

    override fun runLongTask(from: Long?, param: String): Flow<Long> {
        //val topic = Word.apply(param)
        val topic = param
        return fetchNormalBlockNumber()
            .flatMapMany { to -> reindexTopic(topic, from, to) }
            .map { it.first }
            .asFlow()
    }

    private fun reindexTopic(topic: String, from: Long?, end: Long): Flux<LongRange> {
        return blockchainListenerService.reindex(topic, from ?: 1, end)
    }

    private fun fetchNormalBlockNumber() =
        ethereum.ethBlockNumber().map { it.toLong() } // TODO

    companion object {
        const val TOPIC = "TOPIC"
    }
}