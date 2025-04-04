package com.rarible.blockchain.scanner.hedera.client.rest

import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlock
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlockRequest
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlocksResponse
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTimestampFrom
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTimestampTo
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransaction
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionRequest
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionsResponse
import com.rarible.blockchain.scanner.hedera.client.rest.dto.Links
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaNftResponse
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaToken
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBalanceRequest
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBalanceResponse
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaContractResultsRequest
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaContractResultsResponse
import org.springframework.web.reactive.function.client.WebClient
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference

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

    suspend fun getTransactions(
        from: HederaTimestampFrom,
        to: HederaTimestampTo,
    ): HederaTransactionsResponse {
        val transactions = CopyOnWriteArrayList<HederaTransaction>()
        val nextLink = AtomicReference<Links?>(null)
        do {
            val response = getTransactions(
                HederaTransactionRequest(
                    timestampFrom = from,
                    timestampTo = to,
                    limit = MAX_LIMIT
                ),
                nextLink.get()
            )
            transactions.addAll(response.transactions)
            nextLink.set(response.links)
        } while (nextLink.get()?.next != null)

        return HederaTransactionsResponse(transactions, Links.empty())
    }

    suspend fun getBlockByHashOrNumber(hashOrNumber: String): HederaBlock {
        return get("/api/v1/blocks/{hashOrNumber}", hashOrNumber)
    }

    suspend fun getNftByTokenIdAndSerialNumber(tokenId: String, serialNumber: Long): HederaNftResponse {
        return get("/api/v1/tokens/$tokenId/nfts") {
            queryParam("serialNumber", serialNumber)
            this
        }
    }

    suspend fun getToken(tokenId: String): HederaToken {
        return get("/api/v1/tokens/{tokenId}", tokenId)
    }

    suspend fun getBalances(request: HederaBalanceRequest = HederaBalanceRequest()): HederaBalanceResponse {
        return get("/api/v1/balances") {
            request.accountId?.let { queryParam("account.id", it) }
            request.limit?.let { queryParam("limit", it.coerceIn(1, MAX_LIMIT)) }
            request.order?.let { queryParam("order", it.value) }
            this
        }
    }

    suspend fun getContractResults(
        request: HederaContractResultsRequest = HederaContractResultsRequest(),
        nextLink: Links? = null
    ): HederaContractResultsResponse {
        if (nextLink?.next != null) {
            return get(nextLink.next)
        }
        return get("/api/v1/contracts/results") {
            request.from?.let { queryParam("from", it) }
            request.blockHash?.let { queryParam("block.hash", it) }
            request.blockNumber?.let { queryParam("block.number", it) }
            queryParam("internal", request.internal)
            queryParam("limit", request.limit.coerceIn(1, MAX_LIMIT))
            queryParam("order", request.order.value)
            request.timestamp?.forEach { queryParam("timestamp", it) }
            request.transactionIndex?.let { queryParam("transaction.index", it) }
            this
        }
    }

    companion object {
        const val MAX_LIMIT = 100
    }
}
