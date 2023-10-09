package com.rarible.blockchain.scanner.flow.service

import com.nftco.flow.sdk.AsyncFlowAccessApi
import com.nftco.flow.sdk.FlowId
import com.nftco.flow.sdk.FlowTransactionResult
import java.util.concurrent.CompletableFuture

// TODO replace with lib version when onflow actualize it
interface AsyncFlowAccessApi : AsyncFlowAccessApi {
    fun getTransactionResultsByBlockId(id: FlowId): CompletableFuture<List<FlowTransactionResult>>

    fun withSessionHash(sessionHash: String): com.rarible.blockchain.scanner.flow.service.AsyncFlowAccessApi
}
