package com.rarible.blockchain.scanner.flow

import com.nftco.flow.sdk.*
import com.nftco.flow.sdk.impl.AsyncFlowAccessApiImpl
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.future.await
import net.devh.boot.grpc.client.inject.GrpcClient
import org.onflow.protobuf.access.AccessAPIGrpc
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import javax.annotation.PostConstruct

@FlowPreview
@ExperimentalCoroutinesApi
@Component
class DefaultFlowGrpcApi : FlowGrpcApi {

    private val logger: Logger = LoggerFactory.getLogger(DefaultFlowGrpcApi::class.java)

    @GrpcClient("flow")
    private lateinit var stub: AccessAPIGrpc.AccessAPIFutureStub

    private lateinit var api: AsyncFlowAccessApi

    @PostConstruct
    fun postCreate() {
        api = AsyncFlowAccessApiImpl(stub)
    }

    override suspend fun isAlive(): Boolean = try {
        api.ping().await()
        true
    } catch (e: Exception) {
        logger.debug("Network is unreachable: ${e.message}", e)
        false
    }

    override suspend fun latestBlock(): FlowBlock = api.getLatestBlock().await()

    override suspend fun blockByHeight(height: Long): FlowBlock? = api.getBlockByHeight(height).await()

    override suspend fun blockById(id: String): FlowBlock? = api.getBlockById(FlowId(id)).await()

    override suspend fun blockById(id: FlowId): FlowBlock? = api.getBlockById(id).await()

    override suspend fun txById(id: String): FlowTransaction? = api.getTransactionById(FlowId(id)).await()

    override suspend fun eventsByBlockRange(type: String, range: LongRange): Flow<FlowEventResult> =
        api.getEventsForHeightRange(type, range).await().asFlow()

    override suspend fun blockEvents(type: String, block: FlowBlock): Flow<FlowEventResult> {
        if (block.collectionGuarantees.isEmpty()) {
            return emptyFlow()
        }

        return api.getEventsForBlockIds(type, setOf(block.id)).await().asFlow()
    }
}
