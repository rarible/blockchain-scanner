package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.solana.client.dto.ApiResponse
import com.rarible.blockchain.scanner.solana.client.dto.GetBlockRequest
import com.rarible.blockchain.scanner.solana.client.dto.GetBlockRequest.TransactionDetails
import com.rarible.blockchain.scanner.solana.client.dto.GetSlotRequest
import com.rarible.blockchain.scanner.solana.client.dto.GetTransactionRequest
import com.rarible.blockchain.scanner.solana.client.dto.SolanaBlockDto
import com.rarible.blockchain.scanner.solana.client.dto.SolanaTransactionDto
import kotlinx.coroutines.delay
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

interface SolanaApi {
    suspend fun getLatestSlot(): ApiResponse<Long>

    suspend fun getBlock(slot: Long, details: TransactionDetails): ApiResponse<SolanaBlockDto>

    suspend fun getTransaction(signature: String): ApiResponse<SolanaTransactionDto>
}

class SolanaHttpRpcApi(
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

    override suspend fun getLatestSlot(): ApiResponse<Long> = client.post()
        .body(BodyInserters.fromValue(GetSlotRequest))
        .retrieve()
        .bodyToMono<ApiResponse<Long>>()
        .awaitSingle()

    override suspend fun getBlock(slot: Long, details: TransactionDetails): ApiResponse<SolanaBlockDto> {
        val result = client.post()
            .body(BodyInserters.fromValue(GetBlockRequest(slot, details)))
            .retrieve()
            .bodyToMono<ApiResponse<SolanaBlockDto>>()
            .awaitSingle()

        delay(POLLING_DELAY) // TODO remove after getting personal node

        return result
    }

    override suspend fun getTransaction(signature: String) = client.post()
        .body(BodyInserters.fromValue(GetTransactionRequest(signature)))
        .retrieve()
        .bodyToMono<ApiResponse<SolanaTransactionDto>>()
        .awaitSingle()

    companion object {
        const val POLLING_DELAY = 1000L
        const val MAX_BODY_SIZE = 10 * 1024 * 1024
        const val DEFAULT_TIMEOUT = 5000L
    }
}
