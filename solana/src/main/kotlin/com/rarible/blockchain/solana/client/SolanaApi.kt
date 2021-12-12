package com.rarible.blockchain.solana.client

import com.rarible.blockchain.scanner.framework.data.TransactionMeta
import com.rarible.blockchain.solana.client.dto.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.client.ExchangeStrategies
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.bodyToMono
import reactor.netty.http.client.HttpClient
import java.time.Duration

interface SolanaApi {
    fun getBlockFlow(): Flow<SolanaBlockchainBlock>

    suspend fun getLatestSlot(): Long

    suspend fun getBlock(slot: Long): SolanaBlockchainBlock?

    suspend fun getTransaction(signature: String): TransactionMeta?
}

internal class SolanaHttpRpcApi(
    url: String,
    timeoutMillis: Long = DEFAULT_TIMEOUT
) : SolanaApi {
    private val client = WebClient.builder()
        .baseUrl(url)
        .exchangeStrategies(
            ExchangeStrategies.builder()
                .codecs { it.defaultCodecs().maxInMemorySize(MAX_BODY_SIZE) }
                .build()
        )
        .clientConnector(
            ReactorClientHttpConnector(HttpClient.create().responseTimeout(Duration.ofMillis(timeoutMillis)))
        )
        .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
        .build()

    override fun getBlockFlow(): Flow<SolanaBlockchainBlock> = flow {
        var lastSlot: Long = -1

        while (true) {
            val slot = getLatestSlot()

            if (slot != lastSlot) {
                val block = getBlock(slot)

                lastSlot = slot
                block?.let { emit(it) }
            }
            delay(POLLING_DELAY)
        }
    }

    override suspend fun getLatestSlot(): Long = client.post()
        .body(BodyInserters.fromValue(GetSlotRequest))
        .retrieve()
        .bodyToMono<ApiResponse<Long>>()
        .awaitSingle()
        .result

    override suspend fun getBlock(slot: Long): SolanaBlockchainBlock? = client.post()
        .body(BodyInserters.fromValue(GetBlockRequest(slot)))
        .retrieve()
        .bodyToMono<ApiResponse<SolanaBlockDto>>()
        .awaitSingleOrNull()
        ?.result
        ?.toModel(slot)

    override suspend fun getTransaction(signature: String) = client.post()
        .body(BodyInserters.fromValue(GetTransactionRequest(signature)))
        .retrieve()
        .bodyToMono<ApiResponse<SolanaTransactionDto>>()
        .awaitSingleOrNull()
        ?.result
        ?.toModel()

    companion object {
        const val POLLING_DELAY = 500L
        const val MAX_BODY_SIZE = 10 * 1024 * 1024
        const val DEFAULT_TIMEOUT = 2000L
    }
}