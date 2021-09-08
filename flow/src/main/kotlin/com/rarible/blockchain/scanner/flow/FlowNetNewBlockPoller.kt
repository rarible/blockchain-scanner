package com.rarible.blockchain.scanner.flow

import com.nftco.flow.sdk.Flow.DEFAULT_CHAIN_ID
import com.nftco.flow.sdk.FlowBlock
import com.nftco.flow.sdk.FlowChainId
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.flow.retry
import kotlinx.coroutines.future.asDeferred
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicLong

@ObsoleteCoroutinesApi
@ExperimentalCoroutinesApi
@Component
class FlowNetNewBlockPoller(
    @Value("\${blockchain.scanner.flow.chainId}")
    private val chainId: FlowChainId = DEFAULT_CHAIN_ID,
    @Value("\${blockchain.scanner.flow.poller.delay:1000}")
    private val polledDelay: Long
) {

    private val log: Logger = LoggerFactory.getLogger(FlowNetNewBlockPoller::class.java)

    private val start: AtomicLong = AtomicLong(0L)

    /**
     * Run polling from determined block height
     */
    suspend fun poll(fromHeight: Long): Flow<FlowBlock> {
        log.debug("Poll ... from: $fromHeight")
        start.set(fromHeight)
        return channelFlow {
            while (!isClosedForSend) {
                val startNumber = start.get()
                log.debug("try to read block $startNumber")
                val client = FlowAccessApiClientManager.async(startNumber, chainId)
                val latest = client.getLatestBlock(true).asDeferred().await()
                if (latest.height <= startNumber) {
                    log.debug("Latest height on chain greater than need [${latest.height} <= $startNumber]")
                    continue
                }
                val range = (startNumber .. latest.height).asFlow()
                log.debug("read block range $range")
                range.collect {
                    val b = client.getBlockByHeight(it).asDeferred().await()
                    if (b != null) {
                        log.debug("Send to flow ${b.height}")
                        send(b)
                    }
                }
                start.set(latest.height)
                log.debug("Set ${latest.height} as next start value ...")
                delay(polledDelay)
            }
        }.retry {
                if (log.isDebugEnabled) {
                    log.debug("Error in poll function: ${it.message}", it)
                }
                delay(polledDelay)
                true
            }
    }
}
