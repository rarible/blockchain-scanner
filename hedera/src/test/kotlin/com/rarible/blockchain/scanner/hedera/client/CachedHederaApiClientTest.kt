package com.rarible.blockchain.scanner.hedera.client

import com.rarible.blockchain.scanner.hedera.client.data.createRandomHederaBlock
import com.rarible.blockchain.scanner.hedera.client.data.createRandomHederaTransaction
import com.rarible.blockchain.scanner.hedera.client.rest.HederaRestApiClient
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlockRequest
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlocksResponse
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTimestampFrom
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTimestampTo
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionsResponse
import com.rarible.blockchain.scanner.hedera.client.rest.dto.Links
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CachedHederaApiClientTest {

    private val hederaRestApiClient = mockk<HederaRestApiClient>()
    private val client = CachedHederaApiClient(hederaRestApiClient)

    @Test
    fun `should cache transactions`() = runBlocking {
        val from = HederaTimestampFrom.Gte("1")
        val to = HederaTimestampTo.Lte("2")
        val transaction = createRandomHederaTransaction()
        val response = HederaTransactionsResponse(
            transactions = listOf(transaction),
            links = Links.empty()
        )

        coEvery {
            hederaRestApiClient.getTransactions(from = from, to = to)
        } returns response

        // First call - should hit the API
        val result1 = client.getTransactions(from, to)
        assertThat(result1).hasSize(1)
        assertThat(result1[0]).isEqualTo(transaction)

        // Second call within cache TTL - should return cached result
        val result2 = client.getTransactions(from, to)
        assertThat(result2).hasSize(1)
        assertThat(result2[0]).isEqualTo(transaction)

        // Verify API was called only once
        coVerify(exactly = 1) {
            hederaRestApiClient.getTransactions(from = from, to = to)
        }
    }

    @Test
    fun `should pass through block requests`() = runBlocking {
        val request = HederaBlockRequest()
        val block = createRandomHederaBlock()
        val response = HederaBlocksResponse(
            blocks = listOf(block),
            links = Links.empty()
        )

        coEvery {
            hederaRestApiClient.getBlocks(request)
        } returns response

        val result = client.getBlocks(request)
        assertThat(result.blocks).hasSize(1)
        assertThat(result.blocks[0]).isEqualTo(block)

        coVerify(exactly = 1) {
            hederaRestApiClient.getBlocks(request)
        }
    }

    @Test
    fun `should pass through block by hash or number requests`() = runBlocking {
        val hashOrNumber = "123"
        val block = createRandomHederaBlock()

        coEvery {
            hederaRestApiClient.getBlockByHashOrNumber(hashOrNumber)
        } returns block

        val result = client.getBlockByHashOrNumber(hashOrNumber)
        assertThat(result).isEqualTo(block)

        coVerify(exactly = 1) {
            hederaRestApiClient.getBlockByHashOrNumber(hashOrNumber)
        }
    }
}
