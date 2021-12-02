package com.rarible.blockchain.solana.client

import com.fasterxml.jackson.databind.DeserializationFeature
import com.rarible.blockchain.scanner.framework.data.TransactionMeta
import com.rarible.blockchain.solana.client.dto.*
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.features.*
import io.ktor.client.features.json.*
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

interface SolanaApi {
    fun getBlockFlow(): Flow<SolanaBlockchainBlock>

    suspend fun getLatestSlot(): Long

    suspend fun getBlock(slot: Long): SolanaBlockchainBlock?

    suspend fun getTransaction(signature: String): TransactionMeta?
}

internal class SolanaHttpRpcApi(
    private val url: String,
    private val timeoutMillis: Long = 2000
) : SolanaApi {
    private val client = HttpClient(CIO) {
        install(HttpTimeout) {
            requestTimeoutMillis = timeoutMillis
        }
        install(JsonFeature) {
            serializer = JacksonSerializer {
                disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            }
        }
        defaultRequest {
            contentType(ContentType.Application.Json)
        }
    }

    override suspend fun getLatestSlot(): Long = client.post<ApiResponse<Long>>(url) {
        body = GetSlotRequest()
    }.result

    override fun getBlockFlow(): Flow<SolanaBlockchainBlock> = flow {
        while (true) {
            val slot = getLatestSlot()
            val block = getBlock(slot)

            block?.let { emit(it) }
            delay(POLLING_DELAY)
        }
    }

    override suspend fun getBlock(slot: Long) = client.post<ApiResponse<SolanaBlockDto>?>(url) {
        body = GetBlockRequest(slot, transactionDetails = GetBlockRequest.TransactionDetails.None)
    }?.result?.toModel()

    override suspend fun getTransaction(signature: String) = client.post<ApiResponse<SolanaTransactionDto>?>(url) {
        body = GetTransactionRequest(signature)
    }?.result?.toModel()

    companion object {
        const val POLLING_DELAY = 500L
    }
}