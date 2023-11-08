package com.rarible.blockchain.scanner.flow.service

import com.nftco.flow.sdk.FlowId
import com.nftco.flow.sdk.FlowTransactionResult
import com.nftco.flow.sdk.impl.completableFuture
import com.rarible.blockchain.scanner.flow.model.FlowBlockHeader
import io.grpc.ManagedChannel
import io.grpc.Metadata
import io.grpc.stub.MetadataUtils
import org.onflow.protobuf.access.Access
import org.onflow.protobuf.access.AccessAPIGrpc
import java.io.Closeable
import java.util.concurrent.CompletableFuture

class AsyncFlowAccessApiImpl(
    private val api: AccessAPIGrpc.AccessAPIFutureStub,
) : AsyncFlowAccessApi, com.nftco.flow.sdk.AsyncFlowAccessApi by com.nftco.flow.sdk.impl.AsyncFlowAccessApiImpl(api),
    Closeable {

    override fun getTransactionResultsByBlockId(id: FlowId): CompletableFuture<List<FlowTransactionResult>> {
        return completableFuture(
            api.getTransactionResultsByBlockID(
                Access.GetTransactionsByBlockIDRequest.newBuilder()
                    .setBlockId(id.byteStringValue)
                    .build()
            )
        ).thenApply {
            it.transactionResultsList.map { tr -> FlowTransactionResult.of(tr) }
        }
    }

    override fun withSessionHash(sessionHash: String): AsyncFlowAccessApi {
        val metadata = Metadata()
        metadata.put(SESSION_HASH_HEADER, sessionHash)
        return AsyncFlowAccessApiImpl(
            api.withInterceptors(
                MetadataUtils.newAttachHeadersInterceptor(
                    metadata
                )
            )
        )
    }

    override fun getExBlockHeaderById(id: FlowId): CompletableFuture<FlowBlockHeader?> {
        return completableFuture(
            api.getBlockHeaderByID(
                Access.GetBlockHeaderByIDRequest.newBuilder()
                    .setId(id.byteStringValue)
                    .build()
            )
        ).thenApply {
            if (it.hasBlock()) {
                FlowBlockHeader.of(it.block)
            } else {
                null
            }
        }
    }

    override fun getExBlockHeaderByHeight(height: Long): CompletableFuture<FlowBlockHeader?> {
        return completableFuture(
            api.getBlockHeaderByHeight(
                Access.GetBlockHeaderByHeightRequest.newBuilder()
                    .setHeight(height)
                    .build()
            )
        ).thenApply {
            if (it.hasBlock()) {
                FlowBlockHeader.of(it.block)
            } else {
                null
            }
        }
    }

    override fun close() {
        val chan = api.channel
        if (chan is ManagedChannel) {
            chan.shutdownNow()
        }
    }
}
