package com.rarible.blockchain.scanner.hedera.client

import com.rarible.blockchain.scanner.framework.model.ReceivedBlock
import com.rarible.blockchain.scanner.hedera.client.data.createRandomHederaBlock
import com.rarible.blockchain.scanner.hedera.client.data.createRandomHederaBlockchainBlock
import com.rarible.blockchain.scanner.hedera.client.data.createRandomHederaTransaction
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlocksResponse
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaOrder
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTimestampFrom
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTimestampTo
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionType
import com.rarible.blockchain.scanner.hedera.client.rest.dto.Links
import com.rarible.blockchain.scanner.hedera.configuration.BlockchainClientProperties
import com.rarible.blockchain.scanner.hedera.model.HederaDescriptor
import com.rarible.blockchain.scanner.hedera.model.HederaLogStorage
import com.rarible.blockchain.scanner.hedera.model.HederaTransactionFilter
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

@ExperimentalCoroutinesApi
class HederaBlockchainClientTest {

    private val hederaApiClient = mockk<CachedHederaApiClient>()
    private val properties = BlockchainClientProperties()
    private val poller = mockk<HederaNewBlockPoller>()
    private val client = HederaBlockchainClient(hederaApiClient, poller, properties)

    @Test
    fun `get block by number`() = runBlocking<Unit> {
        val block = createRandomHederaBlock()

        coEvery {
            hederaApiClient.getBlockByHashOrNumber(block.number.toString())
        } returns block

        val result = client.getBlock(block.number)
        assertThat(result).isNotNull
        assertThat(result!!.number).isEqualTo(block.number)
        assertThat(result.hash).isEqualTo(block.hash)
        assertThat(result.parentHash).isEqualTo(block.previousHash)
        assertThat(result.consensusTimestampFrom).isEqualTo(block.timestamp.from)
        assertThat(result.consensusTimestampTo).isEqualTo(block.timestamp.to)
    }

    @Test
    fun `get multiple blocks`() = runBlocking<Unit> {
        val block1 = createRandomHederaBlock()
        val block2 = createRandomHederaBlock()

        coEvery {
            hederaApiClient.getBlockByHashOrNumber(block1.number.toString())
        } returns block1

        coEvery {
            hederaApiClient.getBlockByHashOrNumber(block2.number.toString())
        } returns block2

        val result = client.getBlocks(listOf(block1.number, block2.number))
        assertThat(result).hasSize(2)
        assertThat(result.map { it.number }).containsExactlyInAnyOrder(block1.number, block2.number)
    }

    @Test
    fun `get first available block`() = runBlocking<Unit> {
        val block = createRandomHederaBlock()
        val response = HederaBlocksResponse(
            blocks = listOf(block),
            links = Links.empty()
        )

        coEvery {
            hederaApiClient.getBlocks(match {
                it.limit == 1 && it.order == HederaOrder.ASC
            })
        } returns response

        val result = client.getFirstAvailableBlock()
        assertThat(result.number).isEqualTo(block.number)
        assertThat(result.hash).isEqualTo(block.hash)
    }

    @Test
    fun `get last block number`() = runBlocking<Unit> {
        val block = createRandomHederaBlock()
        val response = HederaBlocksResponse(
            blocks = listOf(block),
            links = Links.empty()
        )

        coEvery {
            hederaApiClient.getBlocks(match {
                it.limit == 1 && it.order == HederaOrder.DESC
            })
        } returns response

        val result = client.getLastBlockNumber()
        assertThat(result).isEqualTo(block.number)
    }

    @Test
    fun `get block logs`() = runBlocking<Unit> {
        val type = HederaTransactionType.CRYPTO_TRANSFER.value
        val block = createRandomHederaBlock()
        val transaction = createRandomHederaTransaction(name = type)
        val descriptor = TestHederaDescriptor(type)

        coEvery {
            hederaApiClient.getTransactions(
                from = HederaTimestampFrom.Gte(block.timestamp.from),
                to = HederaTimestampTo.Lte(block.timestamp.to)
            )
        } returns listOf(transaction)

        val blockchainBlock = block.toBlockchainBlock()
        val result = client.getBlockLogs(descriptor, listOf(blockchainBlock), true).toList()

        assertThat(result).hasSize(1)
        assertThat(result[0].block).isEqualTo(blockchainBlock)
        assertThat(result[0].logs).hasSize(1)

        val log = result[0].logs[0]
        assertThat(log.transaction).isEqualTo(transaction)
        assertThat(log.log.blockNumber).isEqualTo(block.number)
        assertThat(log.log.blockHash).isEqualTo(block.hash)
        assertThat(log.log.transactionHash).isEqualTo(transaction.transactionHash)
    }

    @Test
    fun `emit new blocks`() = runBlocking<Unit> {
        val block = createRandomHederaBlockchainBlock()
        every {
            poller.newBlocks()
        } returns flowOf(ReceivedBlock(block))

        val result = client.newBlocks.first()
        assertThat(result).isNotNull
        assertThat(result.number).isEqualTo(block.number)
        assertThat(result.hash).isEqualTo(block.hash)
    }

    private class TestHederaDescriptor(
        transactionType: String
    ) : HederaDescriptor(
        filter = HederaTransactionFilter.ByTransactionType(transactionType),
        id = "test",
        groupId = "test-group",
        storage = mockk<HederaLogStorage>()
    )
}
