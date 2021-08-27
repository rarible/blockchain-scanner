package com.rarible.blockchainscanner.flow

import com.nftco.flow.sdk.Flow
import com.nftco.flow.sdk.FlowChainId
import com.rarible.blockchain.scanner.flow.FlowAccessApiClientManager
import com.rarible.blockchain.scanner.flow.FlowNetNewBlockPoller
import com.rarible.blockchain.scanner.flow.client.FlowClient
import com.rarible.core.test.containers.KGenericContainer
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.TestCoroutineDispatcher
import kotlinx.coroutines.test.runBlockingTest
import org.bouncycastle.util.encoders.Hex
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.MountableFile

@ExperimentalCoroutinesApi
@Testcontainers
internal class ClientTest {

    companion object {

        private const val GRPC_PORT = 3569

        @Container
        val flowEmulator: KGenericContainer = KGenericContainer(
            "zolt85/flow-cli-emulator:latest"
        ).withEnv("FLOW_VERBOSE", "true").withEnv("FLOW_BLOCKTIME", "100ms")
            .withCopyFileToContainer(MountableFile.forClasspathResource("com/rarible/blockchainscanner/flow/flow.json"), "/home/flow-emulator/flow.json")
            .withExposedPorts(GRPC_PORT, 8080)
            .withLogConsumer {
                println(it.utf8String)
            }
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

            val flowClient = FlowClient(lastKnownBlockHeight = blockId, chainId = FlowChainId.EMULATOR, poller = FlowNetNewBlockPoller(chainId = FlowChainId.EMULATOR))
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
                FlowClient(lastKnownBlockHeight = header.height, chainId = FlowChainId.EMULATOR, poller = FlowNetNewBlockPoller(chainId = FlowChainId.EMULATOR))
            val actual = flowClient.getBlock(header.id.base16Value)
            Assertions.assertNotNull(actual)
            Assertions.assertEquals(header.id.base16Value, actual.hash)
        }
    }
}