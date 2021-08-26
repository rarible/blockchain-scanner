package com.rarible.blockchain.scanner.flow

import com.nftco.flow.sdk.Flow.DEFAULT_CHAIN_ID
import com.nftco.flow.sdk.FlowBlock
import com.nftco.flow.sdk.FlowChainId
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.retry
import kotlinx.coroutines.future.await
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

@ObsoleteCoroutinesApi
@ExperimentalCoroutinesApi
@Component
class FlowNetNewBlockPoller(
    private val dispatcher: CoroutineDispatcher = Executors.newCachedThreadPool().asCoroutineDispatcher(),
    @Value("\${blockchain.scanner.flow.chainId}")
    private val chainId: FlowChainId = DEFAULT_CHAIN_ID
) {

    private val log: Logger = LoggerFactory.getLogger(FlowNetNewBlockPoller::class.java)

    private val start: AtomicLong = AtomicLong(0L)

    /**
     * Run polling from determined block height
     */
    suspend fun poll(fromHeight: Long): Flow<FlowBlock> {
        start.set(fromHeight)
        return channelFlow {
            while (!isClosedForSend) {
                val client = FlowAccessApiClientManager.async(start.get(), chainId)
                val b = client.getBlockByHeight(start.get()).await()
                if (b != null) {
                    start.set(start.incrementAndGet())
                    send(b)
                }
                delay(1000L)
            }
        }.retry {
            if (log.isDebugEnabled) {
                log.debug("Error in poll function: ${it.message}", it)
            }
            delay(1000L)
            true
        }
    }

    fun cancel() = dispatcher.cancel()
}
