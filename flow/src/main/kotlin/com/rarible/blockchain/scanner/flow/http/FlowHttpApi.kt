package com.rarible.blockchain.scanner.flow.http

import com.nftco.flow.sdk.FlowEventResult
import kotlinx.coroutines.flow.Flow

interface FlowHttpApi {
    suspend fun eventsByBlockRange(type: String, range: LongRange): Flow<FlowEventResult>
}
