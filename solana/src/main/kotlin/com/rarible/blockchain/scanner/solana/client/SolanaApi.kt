package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.solana.client.dto.ApiResponse
import com.rarible.blockchain.scanner.solana.client.dto.GetBlockRequest
import com.rarible.blockchain.scanner.solana.client.dto.GetBlockRequest.TransactionDetails
import com.rarible.blockchain.scanner.solana.client.dto.GetFirstAvailableBlockRequest
import com.rarible.blockchain.scanner.solana.client.dto.GetSlotRequest
import com.rarible.blockchain.scanner.solana.client.dto.GetTransactionRequest
import com.rarible.blockchain.scanner.solana.client.dto.SolanaBlockDto
import com.rarible.blockchain.scanner.solana.client.dto.SolanaTransactionDto
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.client.ExchangeStrategies
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.bodyToMono
import reactor.netty.http.client.HttpClient
import java.time.Duration
import kotlin.random.Random

interface SolanaApi {
    suspend fun getFirstAvailableBlock(): ApiResponse<Long>

    suspend fun getLatestSlot(): ApiResponse<Long>

    suspend fun getBlock(slot: Long, details: TransactionDetails): ApiResponse<SolanaBlockDto>

    suspend fun getTransaction(signature: String): ApiResponse<SolanaTransactionDto>
}

class SolanaHttpRpcApi(
    private val urls: List<String>,
    timeoutMillis: Long = DEFAULT_TIMEOUT
) : SolanaApi {
    private val uri
        get() = urls[Random.nextInt(urls.size)]

    private val client = WebClient.builder()
        .exchangeStrategies(
            ExchangeStrategies.builder()
                .codecs { it.defaultCodecs().maxInMemorySize(MAX_BODY_SIZE) }
                .build()
        )
        .clientConnector(
            ReactorClientHttpConnector(
                HttpClient.create()
                    .responseTimeout(Duration.ofMillis(timeoutMillis))
                    .compress(true)
            )
        )
        .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
        .build()

    override suspend fun getFirstAvailableBlock(): ApiResponse<Long> = client.post()
        .uri(uri)
        .body(BodyInserters.fromValue(GetFirstAvailableBlockRequest))
        .retrieve()
        .bodyToMono<ApiResponse<Long>>()
        .awaitSingle()

    override suspend fun getLatestSlot(): ApiResponse<Long> = client.post()
        .uri(uri)
        .body(BodyInserters.fromValue(GetSlotRequest))
        .retrieve()
        .bodyToMono<ApiResponse<Long>>()
        .awaitSingle()

    override suspend fun getBlock(slot: Long, details: TransactionDetails): ApiResponse<SolanaBlockDto> = client.post()
        .uri(uri)
        .body(BodyInserters.fromValue(GetBlockRequest(slot, details)))
        .retrieve()
        .bodyToMono<ApiResponse<SolanaBlockDto>>()
        .awaitSingle()

    override suspend fun getTransaction(signature: String) = client.post()
        .uri(uri)
        .body(BodyInserters.fromValue(GetTransactionRequest(signature)))
        .retrieve()
        .bodyToMono<ApiResponse<SolanaTransactionDto>>()
        .awaitSingle()

    companion object {
        const val MAX_BODY_SIZE = 100 * 1024 * 1024
        const val DEFAULT_TIMEOUT = 5000L
    }
}
