package com.rarible.blockchain.scanner.flow

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flowOn
import com.nftco.flow.sdk.Flow.DEFAULT_CHAIN_ID
import com.nftco.flow.sdk.FlowBlock
import com.nftco.flow.sdk.FlowChainId

@ExperimentalCoroutinesApi
class FlowNetNewBlockPoller(
    private val dispatcher: CoroutineDispatcher = Dispatchers.Default,
    private val chainId: FlowChainId = DEFAULT_CHAIN_ID
) {

    /**
     * Run polling from determined block height
     */
    suspend fun poll(fromHeight: Long): Flow<FlowBlock> = channelFlow {
        var start = fromHeight
        while (!isClosedForSend) {
            val d = async {
                val client = FlowAccessApiClientManager.async(start, chainId)
                val b = client.getBlockByHeight(start++).join()
                if (b != null) {
                    send(b)
                }
                delay(500)
            }
            d.await()
        }
    }.flowOn(dispatcher)

    fun cancel() = dispatcher.cancel()
}
