package com.rarible.blockchain.scanner.flow

import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerProperties
import com.rarible.blockchain.scanner.flow.model.ReceivedFlowBlock
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.retry
import kotlinx.coroutines.time.delay
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class FlowNetNewBlockPoller(
    private val properties: FlowBlockchainScannerProperties,
    private val api: FlowGrpcApi
) {
    private val log: Logger = LoggerFactory.getLogger(FlowNetNewBlockPoller::class.java)

    /**
     * Run polling from determined block height
     */
    @ExperimentalCoroutinesApi
    suspend fun poll(): Flow<ReceivedFlowBlock> {
        return channelFlow {
            while (isClosedForSend.not()) {
                val block = api.latestBlock()
                log.info("Send new block ${block.height}")
                send(ReceivedFlowBlock(block))
                delay(properties.poller.period)
            }
        }.retry {
            log.error("Error in poll function: ${it.message}", it)
            delay(properties.poller.period)
            true
        }
    }
}
