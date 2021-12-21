package com.rarible.blockchain.scanner.flow

import com.nftco.flow.sdk.FlowBlock
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.retry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicLong

@Component
class FlowNetNewBlockPoller(
    @Value("\${blockchain.scanner.flow.poller.delay:1000}")
    private val polledDelay: Long,
    private val api: FlowGrpcApi
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
                val latest = api.latestBlock()
                if (latest.height <= startNumber) {
                    log.debug("Latest height on chain greater than need [${latest.height} <= $startNumber]")
                    delay(polledDelay)
                    continue
                }
                val range = (startNumber .. latest.height).asFlow()
                log.debug("read block range ${(startNumber .. latest.height)}")
                range.collect {
                    val block = api.blockByHeight(it)
                    if (block != null) {
                        log.debug("Send to flow ${block.height}")
                        send(block)
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
