package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

internal class SolanaClientTest {
    private val mainNetBeta = "https://api.mainnet-beta.solana.com"
    private val client = SolanaClient(mainNetBeta)

    @Test
    @Disabled
    fun testParseTransactionEvents() = runBlocking {
        val descriptor = SolanaDescriptor(programId = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s", "", "", Any::class.java)
        val events = client.getBlockLogs(descriptor, 91725442L..91725442L)
            .single()
            .logs
            .map(SolanaBlockchainLog::event)

        assertTrue(events.isNotEmpty())
    }

    @Test
    @Disabled
    fun testGetBlock() = runBlocking {
        val slot = client.getLatestSlot()
        val block = client.getBlock(slot)

        assertNotNull(block)
    }

    @Test
    @Disabled
    fun testBlockFlow() = runBlocking {
        val blocks = client.newBlocks.take(3).toList()

        (1 until blocks.lastIndex).forEach {
            assertTrue(blocks[it - 1].number < blocks[it].number)
        }
    }
}
