package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.framework.model.ReceivedBlock
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.BufferOverflow.DROP_OLDEST
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.time.delay
import org.slf4j.LoggerFactory
import scalether.core.MonoEthereum
import scalether.domain.response.Block
import java.math.BigInteger
import java.time.Duration

@ExperimentalCoroutinesApi
class EthereumNewBlockPoller(
    private val ethereum: MonoEthereum,
    private val pollingDelay: Duration
) : EthereumNewBlockSubscriber {

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun newHeads(): Flow<ReceivedBlock<Block<Word>>> = channelFlow {
        var previousHead: BigInteger? = null
        var previousBlockHash: Word? = null
        logger.info("starting new block poller ...")
        while (isClosedForSend.not()) {
            try {
                val latestHead = ethereum.ethBlockNumber().awaitFirstOrNull()
                if (latestHead != null) {
                    val latestBlock = ethereum.ethGetBlockByNumber(latestHead).awaitFirstOrNull()
                    if (latestBlock != null && (latestHead != previousHead || latestBlock.hash() != previousBlockHash)) {
                        logger.info("got new head=[$latestHead, ${latestBlock.hash()}], previous=[$previousHead, $previousBlockHash]")
                        send(ReceivedBlock(latestBlock))
                        previousHead = latestHead
                        previousBlockHash = latestBlock.hash()
                    }
                }
            } catch (e: Exception) {
                logger.warn("error fetching block head, previous=$previousHead", e)
            }
            delay(pollingDelay)
        }
        logger.info("new block poller closed!")
    }.buffer(0, onBufferOverflow = DROP_OLDEST)
}
