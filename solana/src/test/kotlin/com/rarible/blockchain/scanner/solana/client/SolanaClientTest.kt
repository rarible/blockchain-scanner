package com.rarible.blockchain.scanner.solana.client

import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

class SolanaClientTest {
    private val mainNetBeta = "https://api.mainnet-beta.solana.com"
    private val client = SolanaClient(
        SolanaHttpRpcApi(
            urls = listOf(mainNetBeta),
            timeoutMillis = 30000
        ),
        programIds = emptySet() // All programs.
    )

    @Test
    @Disabled
    fun testGetBlock() = runBlocking {
        val slot = client.getLatestSlot()
        val block = client.getBlock(slot)

        assertNotNull(block)
    }

    @Test
    @Disabled
    fun testGetBlockByNumber() = runBlocking {
        val block = client.getBlock(114371623)
        println(block)
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
