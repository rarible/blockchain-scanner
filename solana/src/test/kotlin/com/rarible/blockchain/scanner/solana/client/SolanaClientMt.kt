package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.solana.client.test.TestSolanaScannerConfiguration
import com.rarible.blockchain.scanner.solana.configuration.SolanaBlockchainScannerProperties
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

@Disabled("for manual run")
class SolanaClientMt {
    private val client = SolanaClient(
        SolanaHttpRpcApi(
            urls = listOf(TestSolanaScannerConfiguration.MAIN_NET_BETA),
            timeoutMillis = 30000
        ),
        properties = SolanaBlockchainScannerProperties(rpcApiUrls = emptyList()),
        filters = emptySet() // All programs.
    )

    private val eclipseTestnetHttpRpcApi = SolanaHttpRpcApi(
        urls = listOf("https://testnet.dev2.eclipsenetwork.xyz"),
        timeoutMillis = 30000
    )

    @Test
    fun testGetBlock() = runBlocking {
        val slot = client.getLatestSlot()
        val block = client.getBlock(slot)

        assertNotNull(block)
    }

    @Test
    fun testGetBlockByNumber() = runBlocking {
        val block = client.getBlock(114371623)
        println(block)
    }

    @Test
    fun testBlockFlow() = runBlocking {
        val blocks = client.newBlocks.take(3).toList()

        (1 until blocks.lastIndex).forEach {
            assertTrue(blocks[it - 1].number < blocks[it].number)
        }
    }

    @Test
    fun testGetAccountBase64Info() = runBlocking<Unit> {
        val result = eclipseTestnetHttpRpcApi.getAccountBase64Info("DDGye6JCGsnV3CXcUwCRyu5W5RiFxBtd2aMpRooxYMpw")
        println(result.result?.value?.data)
    }

    @Test
    fun testGetAccountInfo() = runBlocking<Unit> {
        val result = eclipseTestnetHttpRpcApi.getAccountInfo("Gh7bfxEbU4eTLsAuCDGUBWQudm9aKpzbL8A97RshnSVF")
        println(result.result?.value?.owner)

        val result2 = eclipseTestnetHttpRpcApi.getAccountInfo("GpGD296xWJGsyddoCtTG7H7PXd7xmPVuwHqJcEqaUoYP")
        println(result2.result?.value?.data?.parsed?.info)
    }
}
