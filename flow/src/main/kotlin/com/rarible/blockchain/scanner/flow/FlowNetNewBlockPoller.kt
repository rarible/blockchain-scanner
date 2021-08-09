package com.rarible.blockchain.scanner.flow

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flowOn
import org.onflow.sdk.FlowBlock
import org.onflow.sdk.Flow as FlowSDK

@ExperimentalCoroutinesApi
class FlowNetNewBlockPoller(
    private val dispatcher: CoroutineDispatcher = Dispatchers.Default,
    nodeUrl: String
) {

    private val fcl = FlowSDK.newAccessApi(nodeUrl)

    /**
     * Run polling from determined block height
     */
    fun poll(fromHeight: Long): Flow<FlowBlock> = channelFlow {
        var start = fromHeight
        while (!isClosedForSend) {
            val b = fcl.getBlockByHeight(start++)
            if (b != null) {
                send(b)
            }
            delay(500)
        }
    }.flowOn(dispatcher)

    fun cancel() = dispatcher.cancel()
}
