package com.rarible.blockchain.scanner.hedera.client

import com.rarible.blockchain.scanner.hedera.client.data.createRandomHederaBlock
import com.rarible.blockchain.scanner.hedera.client.rest.HederaRestApiClient
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlocksResponse
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaOrder
import com.rarible.blockchain.scanner.hedera.client.rest.dto.Links
import com.rarible.blockchain.scanner.hedera.configuration.BlockPollerProperties
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration

@ExperimentalCoroutinesApi
class HederaNewBlockPollerTest {

    private val hederaApiClient = mockk<HederaRestApiClient>()
    private val properties = BlockPollerProperties(
        period = Duration.ofMillis(100),
    )
    private val poller = HederaNewBlockPoller(hederaApiClient, properties)

    @Test
    fun `should emit new blocks`() = runBlocking<Unit> {
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

        val result = poller.newBlocks().first()
        assertThat(result.block.number).isEqualTo(block.number)
        assertThat(result.block.hash).isEqualTo(block.hash)
    }

    @Test
    fun `should not emit same block twice`() = runBlocking<Unit> {
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

        val result1 = poller.newBlocks().first()
        assertThat(result1.block.number).isEqualTo(block.number)

        // Second call with same block should not emit
        val newBlock = createRandomHederaBlock()
        val newResponse = HederaBlocksResponse(
            blocks = listOf(newBlock),
            links = Links.empty()
        )

        coEvery {
            hederaApiClient.getBlocks(match {
                it.limit == 1 && it.order == HederaOrder.DESC
            })
        } returns newResponse

        val result2 = poller.newBlocks().first()
        assertThat(result2.block.number).isEqualTo(newBlock.number)
        assertThat(result2.block.hash).isEqualTo(newBlock.hash)
    }
}
