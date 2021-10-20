package com.rarible.blockchain.scanner.flow

import com.nftco.flow.sdk.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.future.await


@ExperimentalCoroutinesApi
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

    override suspend fun chunk(range: LongRange): Flow<LongRange> = flowOf(range)

    override suspend fun blockEvents(type: String, blockId: FlowId): Flow<FlowEventResult> {
        return api.getEventsForBlockIds(type, setOf(blockId)).await().asFlow()
    }

    override suspend fun blockEvents(height: Long): Flow<FlowEvent> {
        val block = api.getBlockByHeight(height).await()!!
        if (block.collectionGuarantees.isEmpty()) {
            return emptyFlow()
        }

        return channelFlow {
            block.collectionGuarantees.forEach {
                val c = api.getCollectionById(it.id).await() ?: throw IllegalStateException("Not found collection guarantee [${it.id}]")
                c.transactionIds.mapNotNull { txId ->
                    api.getTransactionResultById(txId).await()
                }.flatMap { it.events }.forEach {
                    send(it)
                }
            }
        }
    }
}
