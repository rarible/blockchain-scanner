package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.configuration.ClientRetryPolicyProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.data.randomBlockchainBlock
import com.rarible.blockchain.scanner.test.data.randomBlockchainLog
import com.rarible.blockchain.scanner.test.data.randomString
import com.rarible.blockchain.scanner.test.data.testDescriptor1
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration

class RetryableBlockchainClientTest {

    private val retryPolicy = ClientRetryPolicyProperties(
        delay = Duration.ofMillis(10),
        attempts = 3
    )
    private val client: TestBlockchainClient = mockk()
    private lateinit var retryableClient: BlockchainClient<TestBlockchainBlock, TestBlockchainLog, TestDescriptor>

    @BeforeEach
    fun beforeEach() {
        clearMocks(client)
        coEvery { client.newBlocks } returns emptyFlow()
        retryableClient = RetryableBlockchainClient(client, retryPolicy)
    }

    @Test
    fun `get block - all attempts failed`() = runBlocking {
        coEvery { client.getBlock(1L) } throws Exception()

        assertThrows(Exception::class.java) {
            runBlocking { retryableClient.getBlock(1L) }
        }

        // Wrapped by retryable, 3 attempts should be there
        coVerify(exactly = 3) { client.getBlock(1L) }
    }

    @Test
    fun `get block events - all attempts failed`() = runBlocking {
        val descriptor = testDescriptor1()
        val range = LongRange.EMPTY
        var count = 0
        coEvery { client.getBlockEvents(descriptor, range) } returns flow {
            count++
            throw Exception()
        }

        assertThrows(Exception::class.java) {
            runBlocking { retryableClient.getBlockEvents(descriptor, range).toList() }
        }

        // Wrapped by retryable, 3 attempts should be there
        coVerify(exactly = 1) { client.getBlockEvents(descriptor, range) }
        assertEquals(3, count)
    }

    @Test
    fun `get block events - last attempt succeed`() = runBlocking {
        val descriptor = testDescriptor1()
        val range = LongRange(1, 1)
        var count = 0
        val block = randomBlockchainBlock()
        val log = randomBlockchainLog(block, randomString())
        val fullBlock = FullBlock(block, listOf(log))
        coEvery { client.getBlockEvents(descriptor, range) } returns flow {
            count++
            if (count < 3) {
                error("not yet ready")
            }
            emit(fullBlock)
        }

        val result = retryableClient.getBlockEvents(descriptor, range)

        // Wrapped by retryable, 3 attempts should be there
        coVerify(exactly = 1) { client.getBlockEvents(descriptor, range) }
        val list = result.toList()
        assertEquals(1, list.size)
        assertEquals(3, count)
        assertEquals(fullBlock, list[0])
    }

    @Test
    fun `get transaction meta - all attempts failed`() = runBlocking {
        val transactionHash = randomString()
        coEvery { client.getTransactionMeta(transactionHash) } throws Exception()

        assertThrows(Exception::class.java) {
            runBlocking { retryableClient.getTransactionMeta(transactionHash) }
        }

        // Wrapped by retryable, 3 attempts should be there
        coVerify(exactly = 3) { client.getTransactionMeta(transactionHash) }
    }

}
