package com.rarible.blockchain.scanner.hedera.client.rest

import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlockRequest
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaOrder
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTimestampFrom
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTimestampTo
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransaction
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionRequest
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionResult
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionType
import com.rarible.blockchain.scanner.hedera.client.rest.dto.Links
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference

@Disabled("Manual integration test")
class HederaRestApiClientIt {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val client = HederaRestApiClient(
        webClient = WebClientFactory.createClient(
            endpoint = "https://mainnet.mirrornode.hedera.com/",
            headers = emptyMap()
        ).build(),
        metrics = HederaClientMetrics(SimpleMeterRegistry()),
        maxErrorBodyLogLength = 10000,
        slowRequestLatency = Duration.ofSeconds(2)
    )

    @Test
    fun `get blocks`() = runBlocking<Unit> {
        val blocks = client.getBlocks(HederaBlockRequest())
        logger.info("Found blocks: {}", blocks)
    }

    @Test
    fun `get block by hash`() = runBlocking<Unit> {
        // Get blocks first to get a valid hash
        val blocks = client.getBlocks(HederaBlockRequest(limit = 1))
        val hash = blocks.blocks.first().hash

        val block = client.getBlockByHashOrNumber(hash)
        logger.info("Found block by hash: {}", block)
    }

    @Test
    fun `get block by number`() = runBlocking<Unit> {
        // Get blocks first to get a valid number
        val blocks = client.getBlocks(HederaBlockRequest())
        val number = blocks.blocks.first().number.toString()

        val block = client.getBlockByHashOrNumber(number)
        logger.info("Found block by number: {}", block)
    }

    @Test
    fun `get transactions`() = runBlocking<Unit> {
        val transactions = client.getTransactions()
        logger.info("Found transactions: {}", transactions)
    }

    @Test
    fun `get transactions with filter`() = runBlocking<Unit> {
        val filter = HederaTransactionRequest(
            limit = 5,
            order = HederaOrder.DESC,
            transactionType = HederaTransactionType.CRYPTO_TRANSFER,
            result = HederaTransactionResult.SUCCESS
        )
        val transactions = client.getTransactions(filter)
        logger.info("Found filtered transactions: {}", transactions)
    }

    @Test
    fun `get block transactions`() = runBlocking<Unit> {
        // Get a single block
        val blocks = client.getBlocks(HederaBlockRequest(limit = 1))
        val block = blocks.blocks.first()
        logger.info("Found block: {}", block)

        // Get all transactions for this block using timestamp range
        val filter = HederaTransactionRequest(
            timestampFrom = HederaTimestampFrom.Gte(block.timestamp.from),
            timestampTo = HederaTimestampTo.Lte(block.timestamp.to),
            limit = 10,
            order = HederaOrder.ASC,
        )
        val transactions = CopyOnWriteArrayList<HederaTransaction>()
        val nextLink = AtomicReference<Links?>(null)
        do {
            val result = client.getTransactions(filter, nextLink.get())
            logger.info("Found next transactions for block {}: transactions {}", block.number, result.transactions.size)
            transactions.addAll(result.transactions)
            nextLink.set(result.links)
        } while (nextLink.get()?.next != null)
        logger.info("Found total transactions for block {}: {}", block.number, transactions.size)
    }
}
