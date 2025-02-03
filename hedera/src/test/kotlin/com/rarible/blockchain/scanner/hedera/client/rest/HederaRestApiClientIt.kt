package com.rarible.blockchain.scanner.hedera.client.rest

import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaOrder
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionFilter
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionResult
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionType
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.Duration

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
        val blocks = client.getBlocks()
        logger.info("Found blocks: {}", blocks)
    }

    @Test
    fun `get block by hash`() = runBlocking<Unit> {
        // Get blocks first to get a valid hash
        val blocks = client.getBlocks()
        val hash = blocks.blocks.first().hash

        val block = client.getBlockByHashOrNumber(hash)
        logger.info("Found block by hash: {}", block)
    }

    @Test
    fun `get block by number`() = runBlocking<Unit> {
        // Get blocks first to get a valid number
        val blocks = client.getBlocks()
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
        val filter = HederaTransactionFilter(
            limit = 5,
            order = HederaOrder.DESC,
            transactionType = HederaTransactionType.CRYPTO_TRANSFER,
            result = HederaTransactionResult.SUCCESS
        )
        val transactions = client.getTransactions(filter)
        logger.info("Found filtered transactions: {}", transactions)
    }
}
