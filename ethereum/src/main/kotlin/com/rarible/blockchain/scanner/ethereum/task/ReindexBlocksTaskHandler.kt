package com.rarible.blockchain.scanner.ethereum.task

import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.handler.HandlerPlanner
import com.rarible.blockchain.scanner.ethereum.handler.ReindexHandler
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.springframework.stereotype.Component

@Component
@ExperimentalCoroutinesApi
class ReindexBlocksTaskHandler(
    reindexHandler: ReindexHandler,
    reindexHandlerPlanner: HandlerPlanner,
    private val blockchainScannerProperties: EthereumScannerProperties
) : AbstractReindexBlocksTaskHandler(reindexHandler, reindexHandlerPlanner) {

    override val type = "BLOCK_SCANNER_REINDEX_TASK"

    override suspend fun isAbleToRun(param: String): Boolean {
        return param.isNotBlank() && blockchainScannerProperties.task.reindex.enabled
    }
}

