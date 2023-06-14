package com.rarible.blockchain.scanner.flow

import com.github.benmanes.caffeine.cache.AsyncLoadingCache
import com.github.benmanes.caffeine.cache.Caffeine
import com.nftco.flow.sdk.AsyncFlowAccessApi
import com.nftco.flow.sdk.FlowBlock
import com.nftco.flow.sdk.FlowBlockHeader
import com.nftco.flow.sdk.FlowChainId
import com.nftco.flow.sdk.FlowEvent
import com.nftco.flow.sdk.FlowEventResult
import com.nftco.flow.sdk.FlowId
import com.nftco.flow.sdk.FlowTransaction
import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerProperties
import com.rarible.blockchain.scanner.flow.monitoring.BlockchainMonitor
import com.rarible.blockchain.scanner.flow.service.SporkService
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.kotlin.core.publisher.toFlux
import reactor.kotlin.core.publisher.toMono

@Component
class CachedSporksFlowGrpcApi(
    private val sporkService: SporkService,
    private val blockchainMonitor: BlockchainMonitor,
    private val properties: FlowBlockchainScannerProperties,
) : FlowGrpcApi {

    private val fullBlocksByHeight: AsyncLoadingCache<Long, FullBlock> = Caffeine.newBuilder()
        .expireAfterWrite(properties.cacheBlockEvents.expireAfter)
        .maximumSize(properties.cacheBlockEvents.size)
        .buildAsync { key, _ -> getFullBlockByHeight(key).toFuture() }

    private val fullBlocksById: AsyncLoadingCache<FlowId, FullBlock> = Caffeine.newBuilder()
        .expireAfterWrite(properties.cacheBlockEvents.expireAfter)
        .maximumSize(properties.cacheBlockEvents.size)
        .buildAsync { key, _ -> getFullBlockById(key).toFuture() }

    override suspend fun isAlive(): Boolean = try {
        blockchainMonitor.onBlockchainCall(properties.blockchain, "ping")
        sporkService.currentSpork().api.ping()
        true
    } catch (e: Exception) {
        logger.warn("Network is unreachable!: ${e.message}")
        false
    }

    override suspend fun latestBlock(): FlowBlock {
        blockchainMonitor.onBlockchainCall(properties.blockchain, "getLatestBlock")
        return sporkService.currentSpork().api.getLatestBlock(true).await()
    }

    override suspend fun blockByHeight(height: Long): FlowBlock? {
        return fullBlocksByHeight[height].await()?.block
    }

    override suspend fun blockById(id: FlowId): FlowBlock? {
        return fullBlocksById[id].await()?.block
    }

    override suspend fun blockById(id: String): FlowBlock? {
        return blockById(FlowId(id))
    }

    override suspend fun txById(id: String): FlowTransaction? {
        val txId = FlowId(id)
        blockchainMonitor.onBlockchainCall(properties.blockchain, "getTransactionById")
        return sporkService.sporkForTx(txId).api.getTransactionById(txId).await()
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
        blockchainMonitor.onBlockchainCall(properties.blockchain, "getBlockHeaderByHeight")
        return sporkService.spork(height).api.getBlockHeaderByHeight(height).await()
    }

    override fun chunk(range: LongRange): Flow<LongRange> {
        return when (sporkService.chainId) {
            FlowChainId.MAINNET -> range.chunked(250) { it.first()..it.last() }.asFlow()
            FlowChainId.TESTNET -> range.chunked(25) { it.first()..it.last() }.asFlow()
            else -> flowOf(range)
        }
    }

    private fun getFullBlockByHeight(height: Long) = mono {
        val api = sporkService.spork(height).api
        blockchainMonitor.onBlockchainCall(properties.blockchain, "getBlockByHeight")
        val block = sporkService.spork(height).api.getBlockByHeight(height).await() ?: return@mono null
        val events = getBlockEvents(api, block)
        FullBlock(block, events)
    }

    private fun getFullBlockById(id: FlowId) = mono {
        val api = sporkService.spork(id).api
        blockchainMonitor.onBlockchainCall(properties.blockchain, "getBlockById")
        val block = api.getBlockById(id).await() ?: return@mono null
        val events = getBlockEvents(api, block)
        FullBlock(block, events)
    }

    private suspend fun getBlockEvents(api: AsyncFlowAccessApi, block: FlowBlock) = coroutineScope<List<FlowEvent>> {
        block.collectionGuarantees.map { guarantee ->
            async {
                blockchainMonitor.onBlockchainCall(properties.blockchain, "getCollectionById")
                val collections = api.getCollectionById(guarantee.id).await() ?: error(
                    "Can't get collection ${guarantee.id}, for block ${block.height}"
                )
                collections.transactionIds.map { transactionId ->
                    async {
                        blockchainMonitor.onBlockchainCall(properties.blockchain, "getTransactionResultById")
                        api.getTransactionResultById(transactionId).await()?.events ?: error(
                            "Can't get events for tx $transactionId, for block ${block.height}"
                        )
                    }
                }.awaitAll().flatten()
            }
        }.awaitAll().flatten()
    }

    private data class FullBlock(
        val block: FlowBlock,
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