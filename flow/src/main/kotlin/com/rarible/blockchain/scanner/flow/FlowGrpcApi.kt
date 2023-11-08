package com.rarible.blockchain.scanner.flow

import com.nftco.flow.sdk.FlowEvent
import com.nftco.flow.sdk.FlowEventResult
import com.nftco.flow.sdk.FlowId
import com.nftco.flow.sdk.FlowTransaction
import com.rarible.blockchain.scanner.flow.model.FlowBlockHeader
import kotlinx.coroutines.flow.Flow

interface FlowGrpcApi {

    suspend fun isAlive(): Boolean

    suspend fun latestBlock(): FlowBlockHeader

    suspend fun blockByHeight(height: Long): FlowBlockHeader?

    suspend fun blockById(id: String): FlowBlockHeader?

    suspend fun blockById(id: FlowId): FlowBlockHeader?

    suspend fun txById(id: String): FlowTransaction?

    fun eventsByBlockRange(type: String, range: LongRange): Flow<FlowEventResult>

    fun blockEvents(type: String, blockId: FlowId): Flow<FlowEventResult>

    suspend fun blockHeaderByHeight(height: Long): FlowBlockHeader?

    fun chunk(range: LongRange): Flow<LongRange>

    fun blockEvents(height: Long): Flow<FlowEvent>
}
