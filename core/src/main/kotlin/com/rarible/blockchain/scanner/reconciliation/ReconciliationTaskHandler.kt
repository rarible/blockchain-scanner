package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.core.task.TaskHandler
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import org.springframework.stereotype.Component

@Component
class ReconciliationTaskHandler(
    private val blockchainListenerServiceReconciliation: ReconciliationBlockIndexerService<*, *, *, *>,
    private val blockchainClient: BlockchainClient<*, *>
) : TaskHandler<Long> {

    override val type: String
        get() = TOPIC

    override fun runLongTask(from: Long?, param: String): Flow<Long> {
        //val topic = Word.apply(param)
        val topic = param
        return blockchainClient.getLastBlockNumber()
            .flatMapMany { blockchainListenerServiceReconciliation.reindex(topic, from ?: 1, it) }
            .map { it.first }
            .asFlow()
    }

    companion object {
        const val TOPIC = "TOPIC"
    }
}