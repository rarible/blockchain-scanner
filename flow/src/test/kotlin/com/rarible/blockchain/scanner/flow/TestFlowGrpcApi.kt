package com.rarible.blockchain.scanner.flow

import com.nftco.flow.sdk.AsyncFlowAccessApi
import com.nftco.flow.sdk.FlowEvent
import com.nftco.flow.sdk.FlowEventResult
import com.nftco.flow.sdk.FlowId
import com.nftco.flow.sdk.FlowTransaction
import com.rarible.blockchain.scanner.util.flatten
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.future.await

class TestFlowGrpcApi(private val api: AsyncFlowAccessApi) : FlowGrpcApi {
    override suspend fun isAlive(): Boolean = true

    override suspend fun latestBlock(): com.rarible.blockchain.scanner.flow.model.FlowBlockHeader = api
        .getLatestBlock().await().let { com.rarible.blockchain.scanner.flow.model.FlowBlockHeader.of(it) }

    override suspend fun blockByHeight(height: Long): com.rarible.blockchain.scanner.flow.model.FlowBlockHeader? = api
        .getBlockByHeight(height).await()?.let { com.rarible.blockchain.scanner.flow.model.FlowBlockHeader.of(it) }

    override suspend fun blockById(id: String): com.rarible.blockchain.scanner.flow.model.FlowBlockHeader? = blockById(FlowId(id))

    override suspend fun blockById(id: FlowId): com.rarible.blockchain.scanner.flow.model.FlowBlockHeader? = api
        .getBlockById(id).await()?.let { com.rarible.blockchain.scanner.flow.model.FlowBlockHeader.of(it) }

    override suspend fun txById(id: String): FlowTransaction? = api.getTransactionById(FlowId(id)).await()

    override fun eventsByBlockRange(type: String, range: LongRange): Flow<FlowEventResult> = flow {
        api.getEventsForHeightRange(type, range).await().asFlow()
    }

    override suspend fun blockHeaderByHeight(height: Long): com.rarible.blockchain.scanner.flow.model.FlowBlockHeader? {
        return api.getBlockByHeight(height).await()?.let { com.rarible.blockchain.scanner.flow.model.FlowBlockHeader.of(it) }
    }

    override fun chunk(range: LongRange): Flow<LongRange> = flowOf(range)

    override fun blockEvents(type: String, blockId: FlowId): Flow<FlowEventResult> = flatten {
        api.getEventsForBlockIds(type, setOf(blockId)).await().asFlow()
    }

    override fun blockEvents(height: Long): Flow<FlowEvent> = flow {
        val block = api.getBlockByHeight(height).await()!!
        if (block.collectionGuarantees.isNotEmpty()) {
            block.collectionGuarantees.forEach {
                val c = api.getCollectionById(it.id).await()
                    ?: throw IllegalStateException("Not found collection guarantee [${it.id}]")
                c.transactionIds.mapNotNull { txId ->
                    api.getTransactionResultById(txId).await()
                }.flatMap { it.events }.forEach {
                    emit(it)
                }
            }
        }
    }
}
