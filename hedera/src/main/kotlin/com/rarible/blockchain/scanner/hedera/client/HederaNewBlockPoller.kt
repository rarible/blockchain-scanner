package com.rarible.blockchain.scanner.hedera.client

import com.rarible.blockchain.scanner.framework.model.ReceivedBlock
import com.rarible.blockchain.scanner.hedera.client.rest.HederaRestApiClient
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlockRequest
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaOrder
import com.rarible.blockchain.scanner.hedera.configuration.BlockPollerProperties
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.time.delay
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
@ExperimentalCoroutinesApi
class HederaNewBlockPoller(
    private val hederaApiClient: HederaRestApiClient,
    private val properties: BlockPollerProperties
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun newBlocks(): Flow<ReceivedBlock<HederaBlockchainBlock>> = channelFlow {
        var previousBlockNumber: Long? = null
        var previousBlockHash: String? = null
        logger.info("Starting Hedera new block poller...")

        while (isClosedForSend.not()) {
            try {
                val latestBlock = getLatestBlock()
                if (latestBlock != null && (latestBlock.number != previousBlockNumber || latestBlock.hash != previousBlockHash)) {
                    logger.info("Got new block=[${latestBlock.number}, ${latestBlock.hash}], previous=[$previousBlockNumber, $previousBlockHash]")
                    send(ReceivedBlock(latestBlock))
                    previousBlockNumber = latestBlock.number
                    previousBlockHash = latestBlock.hash
                }
            } catch (e: Exception) {
                logger.warn("Error fetching block head, previous=$previousBlockNumber", e)
            }
            delay(properties.period)
        }
        logger.info("Hedera new block poller closed!")
    }.buffer(properties.bufferSize, onBufferOverflow = BufferOverflow.DROP_OLDEST)

    private suspend fun getLatestBlock(): HederaBlockchainBlock? {
        val response = hederaApiClient.getBlocks(LATEST_BLOCK_REQUEST)
        return response.blocks.firstOrNull()?.toBlockchainBlock()
    }

    companion object {
        private val LATEST_BLOCK_REQUEST = HederaBlockRequest(
            limit = 1,
            order = HederaOrder.DESC
        )
    }
}
