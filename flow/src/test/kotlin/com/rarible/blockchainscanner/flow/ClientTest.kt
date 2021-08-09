package com.rarible.blockchainscanner.flow

import com.rarible.blockchain.scanner.flow.client.FlowClient
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.TestCoroutineDispatcher
import kotlinx.coroutines.test.runBlockingTest
import org.bouncycastle.util.encoders.Hex
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.onflow.sdk.Flow
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@ExperimentalCoroutinesApi
internal class ClientTest {

    @Test
    internal fun `should return block by blockHeight`() {
        runBlocking {

            val client = Flow.newAccessApi("access.devnet.nodes.onflow.org")
            val blockId = client.getLatestBlockHeader().height

            val flowClient = FlowClient(nodeUrl = "access.devnet.nodes.onflow.org", lastKnownBlockHeight = blockId)
            val block = flowClient.getBlock(blockId)
            Assertions.assertNotNull(block)
            Assertions.assertEquals(blockId, block.number, "BAD block!")
        }
    }

    @Test
    internal fun `should return block by hash`() {
        runBlocking {
            val client = Flow.newAccessApi("access.devnet.nodes.onflow.org")
            val header = client.getLatestBlockHeader()

            val flowClient =
                FlowClient(nodeUrl = "access.devnet.nodes.onflow.org", lastKnownBlockHeight = header.height)
            val actual = flowClient.getBlock(header.id.base16Value)
            Assertions.assertNotNull(actual)
            Assertions.assertEquals(header.id.base16Value, actual.hash)
        }
    }

    @Test
    internal fun `listen new blocks test`() {
        runBlockingTest {
            val dispatcher = TestCoroutineDispatcher()
            val client = Flow.newAccessApi("access.devnet.nodes.onflow.org")
            val header = client.getLatestBlockHeader()

            val flowClient = FlowClient(
                nodeUrl = "access.devnet.nodes.onflow.org",
                dispatcher = dispatcher,
                lastKnownBlockHeight = header.height
            )

            val flow = flowClient.listenNewBlocks()
            launch {
                val data = flow.take(2).toList()
                Assertions.assertEquals(2, data.size)

                val first = data.first()
                val second = data.last()
                Assertions.assertEquals(header.height, first.number, "block height wrong!")
                Assertions.assertEquals(Hex.toHexString(header.id.bytes), first.hash, "block id wrong!")
                Assertions.assertEquals(Hex.toHexString(header.parentId.bytes), first.parentHash, "block parent id wrong!")

                Assertions.assertTrue(second.number > first.number, "second block height wrong!")
                Assertions.assertEquals(second.number, first.number+1, "second block height wrong!")
            }
            dispatcher.advanceTimeBy(1_000)

            flowClient.stopListenNewBlocks()

        }
    }
}
