package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.client.HaNodeProvider
import com.rarible.blockchain.scanner.client.NodeProvider
import com.rarible.blockchain.scanner.solana.client.dto.ApiResponse
import com.rarible.blockchain.scanner.solana.client.dto.Encoding
import com.rarible.blockchain.scanner.solana.client.dto.GetAccountInfo
import com.rarible.blockchain.scanner.solana.client.dto.GetBalance
import com.rarible.blockchain.scanner.solana.client.dto.GetBlockRequest
import com.rarible.blockchain.scanner.solana.client.dto.GetBlockRequest.TransactionDetails
import com.rarible.blockchain.scanner.solana.client.dto.GetBlockRequest.TransactionDetails.None
import com.rarible.blockchain.scanner.solana.client.dto.GetFirstAvailableBlockRequest
import com.rarible.blockchain.scanner.solana.client.dto.GetSlotRequest
import com.rarible.blockchain.scanner.solana.client.dto.GetTransactionRequest
import com.rarible.blockchain.scanner.solana.client.dto.SolanaAccountBase64InfoDto
import com.rarible.blockchain.scanner.solana.client.dto.SolanaAccountInfoDto
import com.rarible.blockchain.scanner.solana.client.dto.SolanaBalanceDto
import com.rarible.blockchain.scanner.solana.client.dto.SolanaBlockDto
import com.rarible.blockchain.scanner.solana.client.dto.SolanaTransactionDto
import com.rarible.blockchain.scanner.solana.client.dto.getSafeResult
import com.rarible.blockchain.scanner.solana.client.dto.toModel
import com.rarible.core.common.asyncWithTraceId
import io.netty.handler.timeout.ReadTimeoutHandler
import io.netty.handler.timeout.WriteTimeoutHandler
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.boot.web.reactive.function.client.WebClientCustomizer
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.bodyToMono
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
import java.time.Duration
import java.time.Instant.now
import java.util.concurrent.TimeUnit

interface SolanaApi {
    suspend fun getFirstAvailableBlock(): ApiResponse<Long>

    suspend fun getLatestSlot(): ApiResponse<Long>

    suspend fun getBlocks(slots: List<Long>, details: TransactionDetails): Map<Long, ApiResponse<SolanaBlockDto>>

    suspend fun getBlock(slot: Long, details: TransactionDetails): ApiResponse<SolanaBlockDto>

    suspend fun getTransaction(signature: String): ApiResponse<SolanaTransactionDto>

    suspend fun getAccountInfo(address: String): ApiResponse<SolanaAccountInfoDto>

    suspend fun getAccountBase64Info(address: String): ApiResponse<SolanaAccountBase64InfoDto>

    suspend fun getBalance(address: String): ApiResponse<SolanaBalanceDto>
}

class SolanaHttpRpcApi(
    val urls: List<String>,
    val timeoutMillis: Long = DEFAULT_TIMEOUT,
    private val haEnabled: Boolean = false,
    private val monitoringInterval: Duration = Duration.ofSeconds(DEFAULT_MONITORING_INTERVAL_SECONDS),
    private val maxBlockDelay: Duration = Duration.ofMinutes(DEFAULT_MAX_BLOCK_DELAY_MINUTES),
) : SolanaApi {

    private val client = WebClient.builder().apply {
        SolanaRpcApiWebClientCustomizer(
            timeout = Duration.ofMillis(timeoutMillis),
            maxBodySize = MAX_BODY_SIZE
        ).customize(it)
    }.build()

    private val nodeProvider = if (haEnabled) haProvider() else object : NodeProvider<String, String> {
        override suspend fun getNode(): String = urls.random()
    }

    private fun haProvider() = HaNodeProvider(
        localNodeConfigs = urls,
        monitoringInterval = monitoringInterval,
        connect = { it },
        isHealthy = { url, _ ->
            try {
                val slot = getLatestSlot(url).toModel()
                val block = getBlock(url, slot, None).getSafeResult(slot, SolanaBlockDto.errorsToSkip)
                block?.let { (now().epochSecond - it.blockTime) < maxBlockDelay.seconds } ?: false
            } catch (e: Exception) {
                false
            }
        }
    )

    private suspend fun uri() = nodeProvider.getNode()

    override suspend fun getFirstAvailableBlock(): ApiResponse<Long> = client.post()
        .uri(uri())
        .body(BodyInserters.fromValue(GetFirstAvailableBlockRequest))
        .retrieve()
        .bodyToMono<ApiResponse<Long>>()
        .awaitSingle()

    override suspend fun getLatestSlot(): ApiResponse<Long> = getLatestSlot(uri())

    private suspend fun getLatestSlot(uri: String): ApiResponse<Long> = client.post()
        .uri(uri)
        .body(BodyInserters.fromValue(GetSlotRequest))
        .retrieve()
        .bodyToMono<ApiResponse<Long>>()
        .awaitSingle()

    override suspend fun getBlocks(
        slots: List<Long>,
        details: TransactionDetails
    ): Map<Long, ApiResponse<SolanaBlockDto>> =
        coroutineScope {
            slots.map { asyncWithTraceId(context = NonCancellable) { it to getBlock(it, details) } }.awaitAll().toMap()
        }

    override suspend fun getBlock(slot: Long, details: TransactionDetails): ApiResponse<SolanaBlockDto> =
        getBlock(uri(), slot, details)

    private suspend fun getBlock(uri: String, slot: Long, details: TransactionDetails) = client.post()
        .uri(uri)
        .body(BodyInserters.fromValue(GetBlockRequest(slot, details)))
        .retrieve()
        .bodyToMono<ApiResponse<SolanaBlockDto>>()
        .awaitSingle()

    override suspend fun getTransaction(signature: String) = client.post()
        .uri(uri())
        .body(BodyInserters.fromValue(GetTransactionRequest(signature)))
        .retrieve()
        .bodyToMono<ApiResponse<SolanaTransactionDto>>()
        .awaitSingle()

    override suspend fun getAccountInfo(address: String): ApiResponse<SolanaAccountInfoDto> = client.post()
        .uri(uri())
        .body(BodyInserters.fromValue(GetAccountInfo(address, Encoding.JSON_PARSED)))
        .retrieve()
        .bodyToMono<ApiResponse<SolanaAccountInfoDto>>()
        .awaitSingle()

    override suspend fun getAccountBase64Info(address: String): ApiResponse<SolanaAccountBase64InfoDto> = client.post()
        .uri(uri())
        .body(BodyInserters.fromValue(GetAccountInfo(address, Encoding.BASE64)))
        .retrieve()
        .bodyToMono<ApiResponse<SolanaAccountBase64InfoDto>>()
        .awaitSingle()

    override suspend fun getBalance(address: String): ApiResponse<SolanaBalanceDto> = client.post()
        .uri(uri())
        .body(BodyInserters.fromValue(GetBalance(address)))
        .retrieve()
        .bodyToMono<ApiResponse<SolanaBalanceDto>>()
        .awaitSingle()

    companion object {
        const val MAX_BODY_SIZE = 100 * 1024 * 1024
        const val DEFAULT_TIMEOUT = 5000L
        const val DEFAULT_MONITORING_INTERVAL_SECONDS = 10000L
        const val DEFAULT_MAX_BLOCK_DELAY_MINUTES = 10L
    }
}

private class SolanaRpcApiWebClientCustomizer(
    private val timeout: Duration,
    private val maxBodySize: Int
) : WebClientCustomizer {

    override fun customize(webClientBuilder: WebClient.Builder) {
        webClientBuilder.codecs { clientCodecConfigurer ->
            clientCodecConfigurer.defaultCodecs().maxInMemorySize(maxBodySize)
        }
        val provider = ConnectionProvider.builder("solana-connection-provider")
            .maxConnections(200)
            .pendingAcquireMaxCount(-1)
            .maxIdleTime(timeout)
            .maxLifeTime(timeout)
            .lifo()
            .build()

        val client = HttpClient
            .create(provider)
            .doOnConnected {
                it.addHandlerLast(ReadTimeoutHandler(timeout.toMillis(), TimeUnit.MILLISECONDS))
                it.addHandlerLast(WriteTimeoutHandler(timeout.toMillis(), TimeUnit.MILLISECONDS))
            }
            .compress(true)
            .responseTimeout(timeout)
            .followRedirect(true)

        val connector = ReactorClientHttpConnector(client)
        webClientBuilder
            .clientConnector(connector)
            .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
    }
}
