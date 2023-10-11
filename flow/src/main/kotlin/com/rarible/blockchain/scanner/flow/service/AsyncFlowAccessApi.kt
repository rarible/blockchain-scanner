package com.rarible.blockchain.scanner.flow.service

import com.nftco.flow.sdk.AsyncFlowAccessApi
import com.nftco.flow.sdk.FlowId
import com.nftco.flow.sdk.FlowTransactionResult
import java.util.concurrent.CompletableFuture

interface AsyncFlowAccessApi : AsyncFlowAccessApi {
    fun getTransactionResultsByBlockId(id: FlowId): CompletableFuture<List<FlowTransactionResult>>

    fun withSessionHash(sessionHash: String): com.rarible.blockchain.scanner.flow.service.AsyncFlowAccessApi
}
