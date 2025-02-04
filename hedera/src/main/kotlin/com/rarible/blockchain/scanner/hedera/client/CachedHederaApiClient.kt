package com.rarible.blockchain.scanner.hedera.client

import com.github.benmanes.caffeine.cache.AsyncLoadingCache
import com.github.benmanes.caffeine.cache.Caffeine
import com.rarible.blockchain.scanner.hedera.client.rest.HederaRestApiClient
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlock
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlockRequest
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlocksResponse
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTimestampFrom
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTimestampTo
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransaction
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactor.mono
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.CompletableFuture

@Component
class CachedHederaApiClient(
    private val hederaApiClient: HederaRestApiClient
) {
    private val transactionsCache: AsyncLoadingCache<TransactionsKey, List<HederaTransaction>> = Caffeine.newBuilder()
        .expireAfterWrite(Duration.ofSeconds(5))
        .maximumSize(1000)
        .buildAsync { key, _ -> getTransactions(key) }

    suspend fun getBlockByHashOrNumber(hashOrNumber: String): HederaBlock {
        return hederaApiClient.getBlockByHashOrNumber(hashOrNumber)
    }

    suspend fun getBlocks(request: HederaBlockRequest): HederaBlocksResponse {
        return hederaApiClient.getBlocks(request)
    }

    suspend fun getTransactions(
        from: HederaTimestampFrom,
        to: HederaTimestampTo,
    ): List<HederaTransaction> {
        val key = TransactionsKey(from = from, to = to)
        return transactionsCache[key].await()
    }

    private fun getTransactions(
        key: TransactionsKey
    ): CompletableFuture<List<HederaTransaction>> {
        return mono { hederaApiClient.getTransactions(from = key.from, to = key.to).transactions }.toFuture()
    }

    private data class TransactionsKey(
        val from: HederaTimestampFrom,
        val to: HederaTimestampTo,
    )
}
