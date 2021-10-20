package com.rarible.blockchain.scanner.flow

import com.nftco.flow.sdk.*
import kotlinx.coroutines.flow.Flow

interface FlowGrpcApi {

    suspend fun isAlive(): Boolean

    suspend fun latestBlock(): FlowBlock

    suspend fun blockByHeight(height: Long): FlowBlock?

    suspend fun blockById(id: String): FlowBlock?

    suspend fun blockById(id: FlowId): FlowBlock?

    suspend fun txById(id: String): FlowTransaction?

    suspend fun eventsByBlockRange(type: String, range: LongRange): Flow<FlowEventResult>

    suspend fun blockEvents(type: String, blockId: FlowId): Flow<FlowEventResult>

    suspend fun blockHeaderByHeight(height: Long): FlowBlockHeader?

    suspend fun chunk(range: LongRange): Flow<LongRange>

    suspend fun blockEvents(height: Long): Flow<FlowEvent>
}
