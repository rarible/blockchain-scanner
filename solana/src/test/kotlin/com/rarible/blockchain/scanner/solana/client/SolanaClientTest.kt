package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.solana.client.test.TestSolanaScannerConfiguration
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

class SolanaClientTest {
    private val client = SolanaClient(
        SolanaHttpRpcApi(
            urls = listOf(TestSolanaScannerConfiguration.MAIN_NET_BETA),
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
    fun testGetBlockByNumber() = runBlocking {
        val block = client.getBlock(145264532)

        client.getBlockLogs(
            object : SolanaDescriptor(
                programId = SolanaProgramId.SPL_TOKEN_PROGRAM,
                groupId = SubscriberGroup.BALANCE.id,
                id = "balance_transfer_income",
                entityType = SolanaBalanceRecord.TransferIncomeRecord::class.java,
                collection = SubscriberGroup.BALANCE.collectionName
            ) {}
        )
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
