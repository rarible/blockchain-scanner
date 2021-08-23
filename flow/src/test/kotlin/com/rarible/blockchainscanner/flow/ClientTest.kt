package com.rarible.blockchainscanner.flow

import com.rarible.blockchain.scanner.flow.FlowAccessApiClientManager
import com.rarible.blockchain.scanner.flow.client.FlowClient
import com.rarible.core.test.containers.KGenericContainer
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
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import com.nftco.flow.sdk.Flow
import com.nftco.flow.sdk.FlowChainId
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@ExperimentalCoroutinesApi
@Testcontainers
internal class ClientTest {

    companion object {

        private const val GRPC_PORT = 3569

        @Container
        val flowEmulator: KGenericContainer = KGenericContainer(
            "gcr.io/flow-container-registry/emulator"
        ).withEnv("FLOW_VERBOSE", "true").withEnv("FLOW_BLOCKTIME", "500ms")
            .withExposedPorts(GRPC_PORT, 8080)
            .waitingFor(Wait.forHttp("/").forPort(8080).forStatusCode(500))

    }


    @Test
    internal fun `should ping emulator`() {
        Assertions.assertTrue(flowEmulator.isRunning)
        val client = Flow.newAccessApi(flowEmulator.host, flowEmulator.getMappedPort(GRPC_PORT))
        try {
            client.ping()
        } catch (e: Throwable) {
            Assertions.fail(e.message, e)
        }
    }

    @Test
    internal fun `should return block by blockHeight`() {
        runBlocking {
            FlowAccessApiClientManager.sporks[FlowChainId.EMULATOR] = listOf(
                FlowAccessApiClientManager.Spork(
                    from = 0L,
                    nodeUrl = flowEmulator.host,
                    port = flowEmulator.getMappedPort(GRPC_PORT)
                )
            )
            val client = Flow.newAccessApi(flowEmulator.host, flowEmulator.getMappedPort(GRPC_PORT))
            val blockId = client.getLatestBlockHeader().height

            val flowClient = FlowClient(lastKnownBlockHeight = blockId, chainId = FlowChainId.EMULATOR)
            val block = flowClient.getBlock(blockId)
            Assertions.assertNotNull(block)
            Assertions.assertEquals(blockId, block.number, "BAD block!")
        }
    }

    @Test
    internal fun `should return block by hash`() {
        runBlocking {
            FlowAccessApiClientManager.sporks[FlowChainId.EMULATOR] = listOf(
                FlowAccessApiClientManager.Spork(
                    from = 0L, nodeUrl = flowEmulator.host, port = flowEmulator.getMappedPort(
                        GRPC_PORT
                    )
                )
            )
            val client = Flow.newAccessApi(flowEmulator.host, flowEmulator.getMappedPort(GRPC_PORT))
            val header = client.getLatestBlockHeader()

            val flowClient =
                FlowClient(lastKnownBlockHeight = header.height, chainId = FlowChainId.EMULATOR)
            val actual = flowClient.getBlock(header.id.base16Value)
            Assertions.assertNotNull(actual)
            Assertions.assertEquals(header.id.base16Value, actual.hash)
        }
    }

    @Test
    internal fun `listen new blocks test`() {
        runBlockingTest {
            FlowAccessApiClientManager.sporks[FlowChainId.EMULATOR] = listOf(
                FlowAccessApiClientManager.Spork(
                    from = 0L, nodeUrl = flowEmulator.host, port = flowEmulator.getMappedPort(
                        GRPC_PORT
                    )
                )
            )
            val dispatcher = TestCoroutineDispatcher()
            val client = Flow.newAccessApi(flowEmulator.host, flowEmulator.getMappedPort(GRPC_PORT))
            val header = client.getLatestBlockHeader()

            val flowClient = FlowClient(
                dispatcher = dispatcher,
                lastKnownBlockHeight = header.height,
                chainId = FlowChainId.EMULATOR
            )

            val flow = flowClient.listenNewBlocks()
            launch {
                val data = flow.take(2).toList()
                Assertions.assertEquals(2, data.size)

                val first = data.first()
                val second = data.last()
                Assertions.assertEquals(header.height, first.number, "block height wrong!")
                Assertions.assertEquals(Hex.toHexString(header.id.bytes), first.hash, "block id wrong!")
                Assertions.assertEquals(
                    Hex.toHexString(header.parentId.bytes),
                    first.parentHash,
                    "block parent id wrong!"
                )

                Assertions.assertTrue(second.number > first.number, "second block height wrong!")
                Assertions.assertEquals(second.number, first.number + 1, "second block height wrong!")
            }
            dispatcher.advanceTimeBy(1_000)

            flowClient.stopListenNewBlocks()

        }
    }
}
