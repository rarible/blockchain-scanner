package com.rarible.blockchain.scanner.client

import com.rarible.blockchain.scanner.configuration.ClientRetryPolicyProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.data.randomBlockchainBlock
import com.rarible.blockchain.scanner.test.data.randomOriginalLog
import com.rarible.blockchain.scanner.test.data.randomPositiveInt
import com.rarible.blockchain.scanner.test.data.randomString
import com.rarible.blockchain.scanner.test.model.TestCustomLogRecord
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.core.common.nowMillis
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
        increment = Duration.ofMillis(5),
        attempts = 3
    )
    private val client: TestBlockchainClient = mockk()
    private lateinit var retryableClient: BlockchainClient<TestBlockchainBlock, TestBlockchainLog, TestDescriptor>
    private val descriptor = TestDescriptor(
        topic = randomString(),
        collection = randomString(),
        contracts = listOf(randomString(), randomString()),
        entityType = TestCustomLogRecord::class.java
    )

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
    fun `get block logs - all attempts failed`() = runBlocking {
        val blocks = listOf<TestBlockchainBlock>()
        var count = 0
        coEvery { client.getBlockLogs(any(), blocks, true) } returns flow {
            count++
            throw Exception("Bye")
        }

        assertThrows(Exception::class.java) {
            runBlocking { retryableClient.getBlockLogs(descriptor, blocks, true).toList() }
        }

        // Wrapped by retryable, 3 attempts should be there
        coVerify(exactly = 1) { client.getBlockLogs(descriptor, blocks, true) }
        assertEquals(3, count)
    }

    @Test
    fun `get block logs - last attempt succeed`() = runBlocking {
        val blocks = listOf(TestBlockchainBlock(1, "hash", "", 0, nowMillis(), ""))
        var count = 0
        val block = randomBlockchainBlock()
        val log = TestBlockchainLog(randomOriginalLog(block, randomString(), randomPositiveInt()), index = randomPositiveInt())
        val fullBlock = FullBlock(block, listOf(log))
        coEvery { client.getBlockLogs(descriptor, blocks, true) } returns flow {
            count++
            if (count < 3) {
                error("not yet ready")
            }
            emit(fullBlock)
        }

        val result = retryableClient.getBlockLogs(descriptor, blocks, true)

        // Wrapped by retryable, 3 attempts should be there
        coVerify(exactly = 1) { client.getBlockLogs(descriptor, blocks, true) }
        val list = result.toList()
        assertEquals(1, list.size)
        assertEquals(3, count)
        assertEquals(fullBlock, list[0])
    }
}
