package com.rarible.blockchain.scanner.hedera.client.rest

import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlockDetails
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlockRequest
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlocksResponse
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionFilter
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionsResponse
import org.springframework.web.reactive.function.client.WebClient
import java.time.Duration

class HederaRestApiClient(
    webClient: WebClient,
    metrics: HederaClientMetrics,
    maxErrorBodyLogLength: Int = 10000,
    slowRequestLatency: Duration = Duration.ofSeconds(2)
) : AbstractHederaRestClient(webClient, maxErrorBodyLogLength, metrics, slowRequestLatency) {

    suspend fun getBlocks(request: HederaBlockRequest): HederaBlocksResponse {
        return get("/api/v1/blocks") {
            request.limit?.let { queryParam("limit", it.coerceIn(1, MAX_LIMIT)) }
            request.order?.let { queryParam("order", it.value) }
            this
        }
    }

    suspend fun getTransactions(filter: HederaTransactionFilter = HederaTransactionFilter()): HederaTransactionsResponse {
        return get("/api/v1/transactions") {
            filter.timestampFrom?.let { queryParam("timestamp", it.queryValue()) }
            filter.timestampTo?.let { queryParam("timestamp", it.queryValue()) }
            filter.limit?.let { queryParam("limit", it.coerceIn(1, MAX_LIMIT)) }
            filter.order?.let { queryParam("order", it.value) }
            filter.transactionType?.let { queryParam("transactiontype", it.value) }
            filter.result?.let { queryParam("result", it.value) }
            this
        }
    }

    suspend fun getBlockByHashOrNumber(hashOrNumber: String): HederaBlockDetails {
        return get("/api/v1/blocks/{hashOrNumber}", hashOrNumber)
    }

    companion object {
        const val MAX_LIMIT = 100
    }
}
