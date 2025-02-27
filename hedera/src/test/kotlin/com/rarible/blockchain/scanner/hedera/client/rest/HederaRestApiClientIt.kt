package com.rarible.blockchain.scanner.hedera.client.rest

import com.rarible.blockchain.scanner.hedera.client.rest.dto.CustomFees
import com.rarible.blockchain.scanner.hedera.client.rest.dto.CustomRoyaltyFee
import com.rarible.blockchain.scanner.hedera.client.rest.dto.Fee
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBalanceRequest
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlockRequest
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaOrder
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTimestampFrom
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTimestampTo
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionRequest
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionResult
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionType
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat

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
    fun `find block transactions by type`() = runBlocking<Unit> {
        // Get a single block
        val latest = client.getBlocks(HederaBlockRequest(limit = 1)).blocks.single()
        val type = HederaTransactionType.TOKEN_MINT.value
        (latest.number downTo 1).forEach { number ->
            val block = client.getBlockByHashOrNumber(number.toString())
            val transactions = client.getTransactions(
                from = HederaTimestampFrom.Gte(block.timestamp.from),
                to = HederaTimestampTo.Lte(block.timestamp.to)
            )
            transactions.transactions.forEach { tx ->
                if (tx.name == type && tx.nftTransfers?.isNotEmpty() == true) {
                    logger.info(
                        "Found transaction: block={}, txHash={}, consensusTimestamp={}, tx={}",
                        block.number, tx.transactionHash, tx.consensusTimestamp, tx
                    )
                    return@runBlocking
                }
            }
        }
    }

    @Test
    fun `get nft by token id and serial number`() = runBlocking<Unit> {
        val tokenId = "0.0.1274110"
        val serialNumber = 103L
        val nftResponse = client.getNftByTokenIdAndSerialNumber(tokenId, serialNumber)
        logger.info("Found NFT: {}", nftResponse)
        val nft = nftResponse.nfts.firstOrNull()
        require(nft != null) { "NFT not found" }
        require(nft.tokenId == tokenId) { "Unexpected token ID: ${nft.tokenId}" }
        require(nft.serialNumber == serialNumber) { "Unexpected serial number: ${nft.serialNumber}" }
    }

    @Test
    fun `get token - ok`() = runBlocking<Unit> {
        val tokenId = "0.0.1456986"
        val token = client.getToken(tokenId)
        logger.info("Found token: {}", token)

        assertThat(token.tokenId).isEqualTo(tokenId)
        assertThat(token.name).isEqualTo("Wrapped Hbar")
        assertThat(token.symbol).isEqualTo("WHBAR")
        assertThat(token.type).isEqualTo("FUNGIBLE_COMMON")
        assertThat(token.decimals).isEqualTo("8")
        assertThat(token.supplyType).isEqualTo("INFINITE")
        assertThat(token.treasuryAccountId).isEqualTo("0.0.1456985")
        assertThat(token.memo).isEqualTo("SaucerSwap staked WHBAR")
    }

    @Test
    fun `get token with royalties - ok`() = runBlocking<Unit> {
        val tokenId = "0.0.1274110"
        val token = client.getToken(tokenId)
        logger.info("Found token: {}", token)

        assertThat(token.customFees).isEqualTo(
            CustomFees(
                createdTimestamp = "1663381794.059045391",
                royaltyFees = listOf(
                    CustomRoyaltyFee(
                        collectorAccountId = "0.0.517699",
                        amount = Fee(
                            numerator = 50,
                            denominator = 1000
                        )
                    )
                )
            )
        )
    }

    @Test
    fun `get balances - all accounts`() = runBlocking<Unit> {
        val balances = client.getBalances(HederaBalanceRequest(limit = 5))
        logger.info("Found balances: {}", balances)

        assertThat(balances.balances).isNotEmpty
        assertThat(balances.timestamp).isNotEmpty
    }

    @Test
    fun `get balances - specific account`() = runBlocking<Unit> {
        val accountId = "0.0.7723163"
        val balances = client.getBalances(HederaBalanceRequest(accountId = accountId))
        logger.info("Found account balance: {}", balances)

        assertThat(balances.balances).isNotEmpty
        val account = balances.balances.singleOrNull { it.account == accountId }
        assertThat(account).isNotNull
        assertThat(account?.balance).isNotNull
    }
}
