package com.rarible.blockchainscanner.flow

import com.nftco.flow.sdk.Flow
import com.nftco.flow.sdk.FlowChainId
import com.rarible.blockchain.scanner.flow.FlowAccessApiClientManager
import com.rarible.blockchain.scanner.flow.FlowNetNewBlockPoller
import com.rarible.blockchain.scanner.flow.client.FlowClient
import com.rarible.blockchain.scanner.flow.service.LastReadBlock
import com.rarible.core.test.containers.KGenericContainer
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.MountableFile

@ObsoleteCoroutinesApi
@ExperimentalCoroutinesApi
@Testcontainers
internal class ClientTest {

    companion object {

        private const val GRPC_PORT = 3569

        @Container
        val flowEmulator: KGenericContainer = KGenericContainer(
            "zolt85/flow-cli-emulator:27"
        ).withEnv("FLOW_VERBOSE", "true").withEnv("FLOW_BLOCKTIME", "100ms")
            .withCopyFileToContainer(MountableFile.forClasspathResource("com/rarible/blockchainscanner/flow/flow.json"), "/home/flow/flow.json")
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
        Assertions.assertDoesNotThrow {
            client.ping()
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

            val flowClient = FlowClient(chainId = FlowChainId.EMULATOR, poller = FlowNetNewBlockPoller(chainId = FlowChainId.EMULATOR, polledDelay = 1000L), LastReadBlock(blockService = mockk()))
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
                FlowClient(chainId = FlowChainId.EMULATOR, poller = FlowNetNewBlockPoller(chainId = FlowChainId.EMULATOR, polledDelay = 1000L), LastReadBlock(blockService = mockk()))
            val actual = flowClient.getBlock(header.id.base16Value)
            Assertions.assertNotNull(actual)
            Assertions.assertEquals(header.id.base16Value, actual.hash)
        }
    }
}
