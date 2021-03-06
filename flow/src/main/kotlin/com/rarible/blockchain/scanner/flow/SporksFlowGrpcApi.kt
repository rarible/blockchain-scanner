package com.rarible.blockchain.scanner.flow

import com.nftco.flow.sdk.*
import com.rarible.blockchain.scanner.flow.service.SporkService
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Primary
import org.springframework.stereotype.Component
import reactor.kotlin.core.publisher.toFlux
import reactor.kotlin.core.publisher.toMono
import java.util.*

@ExperimentalCoroutinesApi
@FlowPreview
@Component
@Primary
class SporksFlowGrpcApi(
    private val sporkService: SporkService
): FlowGrpcApi {

    private val log: Logger = LoggerFactory.getLogger(SporksFlowGrpcApi::class.java)

    private val blocksByHeight: WeakHashMap<Long, FlowBlock> = WeakHashMap()

    private val blocksById: WeakHashMap<FlowId, FlowBlock> = WeakHashMap()

    private val transactionsById: WeakHashMap<FlowId, FlowTransaction> = WeakHashMap()

    private val headerByHeight: WeakHashMap<Long, FlowBlockHeader> = WeakHashMap()

    private val eventsByHeight: WeakHashMap<Long, List<FlowEvent>> = WeakHashMap()

    override suspend fun isAlive(): Boolean = try {
        sporkService.currentSpork().api.ping()
        true
    } catch (e: Exception) {
        log.warn("Network is unreachable!: ${e.message}")
        false
    }

    override suspend fun latestBlock(): FlowBlock =
        sporkService.currentSpork().api.getLatestBlock(true).await()


    override suspend fun blockByHeight(height: Long): FlowBlock? =
        blocksByHeight.getOrPut(height) {
            sporkService.spork(height).api.getBlockByHeight(height).await()
        }

    override suspend fun blockById(id: String): FlowBlock? = blockById(FlowId(id))

    override suspend fun blockById(id: FlowId): FlowBlock? =
        blocksById.getOrPut(id) {
            sporkService.spork(id).api.getBlockById(id).await()
        }

    override suspend fun txById(id: String): FlowTransaction? {
        val txId = FlowId(id)
        return transactionsById.getOrPut(txId) {
            sporkService.sporkForTx(txId).api.getTransactionById(txId).await()
        }
    }

    override fun eventsByBlockRange(type: String, range: LongRange): Flow<FlowEventResult> {
        return sporkService.sporks(range).flatMapConcat { spork ->
            try {
                spork.api.getEventsForHeightRange(type, spork.trim(range)).await().asFlow()
            } catch (e: StatusRuntimeException) {
                if (e.status.code in arrayOf(Status.INTERNAL.code, Status.UNKNOWN.code)) {
                    range.chunked(5) {
                        it.first()..it.last()
                    }.map { smallRange ->
                        spork.api.getEventsForHeightRange(type, spork.trim(smallRange)).await()
                    }.flatten().asFlow()
                } else {
                    throw e
                }
            }
        }
    }

    override fun blockEvents(type: String, blockId: FlowId): Flow<FlowEventResult> = flow {
        emitAll(sporkService.spork(blockId).api.getEventsForBlockIds(type, setOf(blockId)).await().asFlow())
    }

    override fun blockEvents(height: Long): Flow<FlowEvent> = flow {
        emitAll(
            eventsByHeight.getOrPut(height) {
                val api = sporkService.spork(height).api
                val block = blockByHeight(height)!!
                block.collectionGuarantees.toFlux()
                    .flatMap { api.getCollectionById(it.id).toMono() }
                    .flatMap { it.transactionIds.toFlux() }
                    .flatMap { api.getTransactionResultById(it).toMono() }
                    .flatMap { it.events.toFlux() }
                    .asFlow()
                    .toList()
            }.asFlow()
        )
    }

    override suspend fun blockHeaderByHeight(height: Long): FlowBlockHeader? =
        headerByHeight.getOrPut(height) {
            sporkService.spork(height).api.getBlockHeaderByHeight(height).await()
        }

    override fun chunk(range: LongRange): Flow<LongRange> {
        return when(sporkService.chainId) {
            FlowChainId.MAINNET -> range.chunked(250) { it.first()..it.last() }.asFlow()
            FlowChainId.TESTNET -> range.chunked(25) { it.first()..it.last() }.asFlow()
            else -> flowOf(range)
        }
    }
}
