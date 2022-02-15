package com.rarible.blockchain.scanner.flow

import com.nftco.flow.sdk.*
import com.nftco.flow.sdk.impl.AsyncFlowAccessApiImpl
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.MetadataUtils
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import org.onflow.protobuf.access.AccessAPIGrpc
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import reactor.kotlin.core.publisher.toFlux
import reactor.kotlin.core.publisher.toMono
import javax.annotation.PostConstruct

@Component("alchemy-api")
@ConditionalOnProperty(prefix = "blockchain.scanner.flow", value = ["apiBean"], havingValue = "alchemy-api")
class AlchemyFlowGrpcApi(
    @Value("\${blockchain.scanner.flow.alchemyKey}")
    private val apiKey: String,
    @Value("\${blockchain.scanner.flow.chainId}")
    private val chainId: FlowChainId,
) : FlowGrpcApi {

    private lateinit var api: AsyncFlowAccessApi

    private val apiHeaderKey = io.grpc.Metadata.Key.of("api_key", io.grpc.Metadata.ASCII_STRING_MARSHALLER)

    @Suppress("PrivatePropertyName")
    private val DEFAULT_MESSAGE_SIZE: Int = 33554432 //32 Mb

    private val logger: Logger = LoggerFactory.getLogger(AlchemyFlowGrpcApi::class.java)

    @PostConstruct
    fun init() {
        if (logger.isDebugEnabled) {
            logger.debug("Init Alchemy API client ...")
            logger.debug("Chain-ID: $chainId")
            logger.debug("API Key: $apiKey")
        }
        val data = io.grpc.Metadata()
        data.put(apiHeaderKey, apiKey)
        val channel = ManagedChannelBuilder.forAddress(nodeUrl(), 443)
            .maxInboundMessageSize(DEFAULT_MESSAGE_SIZE)
            .useTransportSecurity()
            .intercept(MetadataUtils.newAttachHeadersInterceptor(data))
            .build()
        val stub = AccessAPIGrpc.newFutureStub(channel)
        api = AsyncFlowAccessApiImpl(stub)

    }

    private fun nodeUrl(): String {
        return when (chainId) {
            FlowChainId.MAINNET -> "flow-mainnet.g.alchemy.com"
            FlowChainId.TESTNET -> "flow-testnet.g.alchemy.com"
            else -> throw IllegalStateException("Unsupported chain-id: $chainId")
        }
    }

    override suspend fun isAlive(): Boolean = try {
        api.ping()
        true
    } catch (e: Exception) {
        logger.warn("Network is unreachable!: ${e.message}")
        false
    }

    override suspend fun latestBlock(): FlowBlock = api.getLatestBlock(true).await()

    override suspend fun blockByHeight(height: Long): FlowBlock? =
        api.getBlockByHeight(height).await()

    override suspend fun blockById(id: String): FlowBlock? = blockById(FlowId(id))

    override suspend fun blockById(id: FlowId): FlowBlock? =
        api.getBlockById(id).await()

    override suspend fun txById(id: String): FlowTransaction? =
        api.getTransactionById(FlowId(id)).await()

    override fun eventsByBlockRange(type: String, range: LongRange): Flow<FlowEventResult> = flow {
        emitAll(
            api.getEventsForHeightRange(type, range).await().asFlow()
        )
    }


    override fun blockEvents(type: String, blockId: FlowId): Flow<FlowEventResult> = flow {
        emitAll(
            api.getEventsForBlockIds(type, setOf(blockId)).await().asFlow()
        )
    }


    override fun blockEvents(height: Long): Flow<FlowEvent> = flow {
        val block = api.getBlockByHeight(height).await()
            ?: throw IllegalStateException("Not found block with height: $height. Chain id: $chainId. Node url: ${nodeUrl()}")
        emitAll(
            block.collectionGuarantees.toFlux()
                .flatMap { api.getCollectionById(it.id).toMono() }
                .flatMap { it.transactionIds.toFlux() }
                .flatMap { api.getTransactionResultById(it).toMono() }
                .flatMap { it.events.toFlux() }
                .asFlow()
        )
    }

    override suspend fun blockHeaderByHeight(height: Long): FlowBlockHeader? =
        api.getBlockHeaderByHeight(height).await()

    override fun chunk(range: LongRange): Flow<LongRange> =
        when(chainId) {
            FlowChainId.MAINNET -> range.chunked(250) { it.first()..it.last() }.asFlow()
            FlowChainId.TESTNET -> range.chunked(25) { it.first()..it.last() }.asFlow()
            else -> flowOf(range)
        }
}
