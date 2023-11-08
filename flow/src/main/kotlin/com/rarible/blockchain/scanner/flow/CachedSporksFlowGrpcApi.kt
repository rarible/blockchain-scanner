package com.rarible.blockchain.scanner.flow

import com.github.benmanes.caffeine.cache.AsyncLoadingCache
import com.github.benmanes.caffeine.cache.Caffeine
import com.nftco.flow.sdk.FlowBlock
import com.nftco.flow.sdk.FlowChainId
import com.nftco.flow.sdk.FlowCollection
import com.nftco.flow.sdk.FlowEvent
import com.nftco.flow.sdk.FlowEventResult
import com.nftco.flow.sdk.FlowId
import com.nftco.flow.sdk.FlowTransaction
import com.nftco.flow.sdk.FlowTransactionResult
import com.rarible.blockchain.scanner.client.AbstractRetryableClient
import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerProperties
import com.rarible.blockchain.scanner.flow.model.FlowBlockHeader
import com.rarible.blockchain.scanner.flow.service.AsyncFlowAccessApi
import com.rarible.blockchain.scanner.flow.service.FlowApiFactory
import com.rarible.blockchain.scanner.flow.service.Spork
import com.rarible.blockchain.scanner.flow.service.SporkService
import com.rarible.core.common.asyncWithTraceId
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import reactor.kotlin.core.publisher.toMono
import java.util.UUID

@Component
class CachedSporksFlowGrpcApi(
    private val sporkService: SporkService,
    private val properties: FlowBlockchainScannerProperties,
    private val flowApiFactory: FlowApiFactory,
) : FlowGrpcApi, AbstractRetryableClient(properties.retryPolicy.client) {

    private val fullBlocksByHeight: AsyncLoadingCache<Long, FullBlock> = Caffeine.newBuilder()
        .expireAfterWrite(properties.cacheBlockEvents.expireAfter)
        .maximumSize(properties.cacheBlockEvents.size)
        .buildAsync { key, _ -> getFullBlockByHeight(key).toFuture() }

    private val fullBlocksById: AsyncLoadingCache<FlowId, FullBlock> = Caffeine.newBuilder()
        .expireAfterWrite(properties.cacheBlockEvents.expireAfter)
        .maximumSize(properties.cacheBlockEvents.size)
        .buildAsync { key, _ -> getFullBlockById(key).toFuture() }

    override suspend fun isAlive(): Boolean = try {
        api(sporkService.currentSpork()).ping().await()
        true
    } catch (e: Exception) {
        logger.warn("Network is unreachable!: ${e.message}")
        false
    }

    override suspend fun latestBlock(): FlowBlockHeader {
        return api(sporkService.currentSpork())
            .getLatestBlock(true).await()
            .let { FlowBlockHeader.of(it) }
    }

    override suspend fun blockByHeight(height: Long): FlowBlockHeader? {
        return fullBlocksByHeight[height].await()?.block
    }

    override suspend fun blockById(id: FlowId): FlowBlockHeader? {
        return fullBlocksById[id].await()?.block
    }

    override suspend fun blockById(id: String): FlowBlockHeader? {
        return blockById(FlowId(id))
    }

    override suspend fun txById(id: String): FlowTransaction? {
        val txId = FlowId(id)
        return api(sporkService.sporkForTx(txId)).getTransactionById(txId).await()
    }

    override fun eventsByBlockRange(type: String, range: LongRange): Flow<FlowEventResult> {
        return range.toFlux()
            .flatMap { fullBlocksByHeight.get(it).toMono() }
            .map { it.getFlowEventResultForType(type) }
            .asFlow()
    }

    override fun blockEvents(type: String, blockId: FlowId): Flow<FlowEventResult> {
        return fullBlocksById[blockId].toMono()
            .map { it.getFlowEventResultForType(type) }
            .asFlow()
    }

    override fun blockEvents(height: Long): Flow<FlowEvent> = flow {
        fullBlocksByHeight[height].await()?.events?.forEach { event ->
            emit(event)
        }
    }

    override suspend fun blockHeaderByHeight(height: Long): FlowBlockHeader? {
        return api(sporkService.spork(height)).getExBlockHeaderByHeight(height).await()
    }

    override fun chunk(range: LongRange): Flow<LongRange> {
        return when (sporkService.chainId) {
            FlowChainId.MAINNET -> range.chunked(250) { it.first()..it.last() }.asFlow()
            FlowChainId.TESTNET -> range.chunked(25) { it.first()..it.last() }.asFlow()
            else -> flowOf(range)
        }
    }

    private fun getFullBlockByHeight(height: Long): Mono<FullBlock> = mono {
        val api = api(sporkService.spork(height)).withSessionHash(UUID.randomUUID().toString())
        getFullBlockByHeight(api, height).awaitSingle()
    }

    private fun getFullBlockByHeight(api: AsyncFlowAccessApi, height: Long) = mono {
        val header = api.getExBlockHeaderByHeight(height).await() ?: return@mono null
        val events = getBlockEvents(api, header)
        FullBlock(header, events)
    }

    private fun getFullBlockById(id: FlowId) = mono {
        val api = api(sporkService.spork(id)).withSessionHash(UUID.randomUUID().toString())
        val header = api.getExBlockHeaderById(id).await() ?: return@mono null
        val events = getBlockEvents(api, header)
        FullBlock(header, events)
    }

    private suspend fun getBlockEvents(api: AsyncFlowAccessApi, header: FlowBlockHeader) = coroutineScope<List<FlowEvent>> {
        if (properties.enableUseUndocumentedMethods) {
            val events = getTransactionResultsByBlockId(api, header.id, header).flatMap { it.events }
            logger.info("Fetched events of block ${header.height}")
            events
        } else {
            val block = api
                .getBlockById(header.id).await()
                ?: return@coroutineScope emptyList()

            logger.info(
                "Fetching events for block ${block.height}" +
                        " with undoc=${properties.enableUseUndocumentedMethods}" +
                        " with seals=${block.blockSeals.map { it.id.base16Value }}" +
                        " and collections=${block.collectionGuarantees.map { it.id.base16Value }}" +
                        " and ${block.signatures.size} signatures"
            )
            block.collectionGuarantees.map { guarantee ->
                asyncWithTraceId(context = NonCancellable) {
                    val collection = getCollectionById(api, guarantee.id, block) ?: error(
                        "Can't get collection ${guarantee.id}, for block ${block.height}"
                    )
                    collection.transactionIds.map { transactionId ->
                        asyncWithTraceId(context = NonCancellable) {
                            val events = getTransactionResultById(api, transactionId, block)?.events ?: error(
                                "Can't get events for tx $transactionId, for block ${block.height}"
                            )
                            // To debug received information in block - it seems like it not full sometimes
                            val counters = events.groupBy { it.type }.mapValues { it.value.size }
                            logger.info(
                                "Fetched events for tx=${transactionId.base16Value} of block ${block.height}" +
                                    " (collection=${collection.id.base16Value}): $counters"
                            )
                            events
                        }
                    }.awaitAll().flatten()
                }
            }.awaitAll().flatten()
        }
    }

    private suspend fun getCollectionById(api: AsyncFlowAccessApi, id: FlowId, block: FlowBlock): FlowCollection? {
        return wrapWithRetry("getCollectionById", id.stringValue, block.height) {
            api.getCollectionById(id).await()
        }
    }

    private suspend fun getTransactionResultById(
        api: AsyncFlowAccessApi,
        id: FlowId,
        block: FlowBlock
    ): FlowTransactionResult? {
        return wrapWithRetry("getTransactionResultById", id.stringValue, block.height) {
            api.getTransactionResultById(id).await()
        }
    }

    private suspend fun getTransactionResultsByBlockId(
        api: AsyncFlowAccessApi,
        id: FlowId,
        block: FlowBlockHeader
    ): List<FlowTransactionResult> {
        return wrapWithRetry("getTransactionResultsByBlockId", id.stringValue, block.height) {
            api.getTransactionResultsByBlockId(id).await()
        }
    }

    private suspend fun api(spork: Spork) = flowApiFactory.getApi(spork)

    private data class FullBlock(
        val block: FlowBlockHeader,
        val events: List<FlowEvent>
    ) {
        fun getFlowEventResultForType(type: String): FlowEventResult {
            return FlowEventResult(
                blockId = block.id,
                blockHeight = block.height,
                blockTimestamp = block.timestamp,
                events = events.filter { event -> event.type == type }
            )
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(CachedSporksFlowGrpcApi::class.java)
    }
}
