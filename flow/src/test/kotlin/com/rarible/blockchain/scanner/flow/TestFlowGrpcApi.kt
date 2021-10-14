package com.rarible.blockchain.scanner.flow

import com.nftco.flow.sdk.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.future.await


class TestFlowGrpcApi(private val api: AsyncFlowAccessApi): FlowGrpcApi {
    override suspend fun isAlive(): Boolean = true

    override suspend fun latestBlock(): FlowBlock = api.getLatestBlock().await()

    override suspend fun blockByHeight(height: Long): FlowBlock? = api.getBlockByHeight(height).await()

    override suspend fun blockById(id: String): FlowBlock? = blockById(FlowId(id))

    override suspend fun blockById(id: FlowId): FlowBlock? = api.getBlockById(id).await()

    override suspend fun txById(id: String): FlowTransaction? = api.getTransactionById(FlowId(id)).await()

    override suspend fun eventsByBlockRange(type: String, range: LongRange): Flow<FlowEventResult> =
        api.getEventsForHeightRange(type, range).await().asFlow()

    override suspend fun blockHeaderByHeight(height: Long): FlowBlockHeader? {
        return api.getBlockHeaderByHeight(height).await()
    }

    override suspend fun blockEvents(type: String, blockId: FlowId): Flow<FlowEventResult> {
        return api.getEventsForBlockIds(type, setOf(blockId)).await().asFlow()
    }
}