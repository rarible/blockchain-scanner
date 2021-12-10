package com.rarible.blockchain.solana.client

import com.rarible.blockchain.solana.client.SolanaBlockEvent.SolanaCreateTokenMetadataEvent
import com.rarible.blockchain.solana.client.SolanaBlockEvent.SolanaMintEvent
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

internal class SolanaClientTest {
    private val mainNetBeta = "https://api.mainnet-beta.solana.com"
    private val client = SolanaClient(mainNetBeta)
    private val api = SolanaHttpRpcApi(mainNetBeta)

    @Test
    fun testParseTransactionEvents() = runBlocking {
        val event = api.getBlockEvents(91725442)

        assertTrue(event.isNotEmpty())
        assertTrue(event.filterIsInstance<SolanaMintEvent>().isNotEmpty())
        assertTrue(event.filterIsInstance<SolanaCreateTokenMetadataEvent>().isNotEmpty())
    }

    @Test
    fun testGetBlock() = runBlocking {
        val slot = client.getLatestSlot()
        val block = client.getBlock(slot)

        assertNotNull(block)
    }

    @Test
    fun testBlockFlow() = runBlocking {
        val blocks = client.newBlocks.take(3).toList()

        (1 until blocks.lastIndex).forEach {
            assertTrue(blocks[it - 1].number < blocks[it].number)
        }
    }
}