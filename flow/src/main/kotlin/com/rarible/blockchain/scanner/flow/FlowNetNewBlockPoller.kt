package com.rarible.blockchain.scanner.flow

import com.nftco.flow.sdk.FlowBlock
import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerProperties
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.retry
import kotlinx.coroutines.time.delay
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class FlowNetNewBlockPoller(
    private val properties: FlowBlockchainScannerProperties,
    private val api: FlowGrpcApi
) {
    private val log: Logger = LoggerFactory.getLogger(FlowNetNewBlockPoller::class.java)

    /**
     * Run polling from determined block height
     */
    @ExperimentalCoroutinesApi
    suspend fun poll(): Flow<FlowBlock> {
        return channelFlow {
            while (isClosedForSend.not()) {
                val latest = api.latestBlock()
                val block = api.blockByHeight(latest.height)
                if (block != null) {
                    log.info("Send new block ${block.height}")
                    send(block)
                } else {
                    log.info("Got null block")
                }
                delay(properties.poller.period)
            }
        }.retry {
            log.error("Error in poll function: ${it.message}", it)
            delay(properties.poller.period)
            true
        }
    }
}
