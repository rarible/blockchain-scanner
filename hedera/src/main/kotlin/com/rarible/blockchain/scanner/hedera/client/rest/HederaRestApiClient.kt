package com.rarible.blockchain.scanner.hedera.client.rest

import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlock
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlockRequest
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlocksResponse
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionRequest
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionsResponse
import com.rarible.blockchain.scanner.hedera.client.rest.dto.Links
import org.springframework.web.reactive.function.client.WebClient
import java.time.Duration

class HederaRestApiClient(
    webClient: WebClient,
    metrics: HederaClientMetrics,
    maxErrorBodyLogLength: Int = 10000,
    slowRequestLatency: Duration = Duration.ofSeconds(2)
) : AbstractHederaRestClient(webClient, maxErrorBodyLogLength, metrics, slowRequestLatency) {

    suspend fun getBlocks(request: HederaBlockRequest = HederaBlockRequest()): HederaBlocksResponse {
        return get("/api/v1/blocks") {
            request.limit?.let { queryParam("limit", it.coerceIn(1, MAX_LIMIT)) }
            request.order?.let { queryParam("order", it.value) }
            this
        }
    }

    suspend fun getTransactions(
        request: HederaTransactionRequest = HederaTransactionRequest(),
        nextLink: Links? = null
    ): HederaTransactionsResponse {
        if (nextLink?.next != null) {
            return get(nextLink.next)
        }
        return get("/api/v1/transactions") {
            request.timestampFrom?.let { queryParam("timestamp", it.queryValue()) }
            request.timestampTo?.let { queryParam("timestamp", it.queryValue()) }
            request.limit?.let { queryParam("limit", it.coerceIn(1, MAX_LIMIT)) }
            request.order?.let { queryParam("order", it.value) }
            request.transactionType?.let { queryParam("transactiontype", it.value) }
            request.result?.let { queryParam("result", it.value) }
            this
        }
    }

    suspend fun getBlockByHashOrNumber(hashOrNumber: String): HederaBlock {
        return get("/api/v1/blocks/{hashOrNumber}", hashOrNumber)
    }

    companion object {
        const val MAX_LIMIT = 100
    }
}
