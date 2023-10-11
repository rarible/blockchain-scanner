package com.rarible.blockchain.scanner.flow.service

import com.nftco.flow.sdk.FlowId
import com.nftco.flow.sdk.FlowTransactionResult
import com.nftco.flow.sdk.impl.completableFuture
import io.grpc.ManagedChannel
import io.grpc.Metadata
import io.grpc.stub.MetadataUtils
import org.onflow.protobuf.access.Access
import org.onflow.protobuf.access.AccessAPIGrpc
import java.io.Closeable
import java.util.concurrent.CompletableFuture

class AsyncFlowAccessApiImpl(
    private val api: AccessAPIGrpc.AccessAPIFutureStub,
) : AsyncFlowAccessApi, com.nftco.flow.sdk.AsyncFlowAccessApi by AsyncFlowAccessApiImpl(api), Closeable {
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

    override fun close() {
        val chan = api.channel
        if (chan is ManagedChannel) {
            chan.shutdownNow()
        }
    }
}
