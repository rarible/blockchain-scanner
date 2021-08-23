package com.rarible.blockchainscanner.flow

import com.rarible.core.test.containers.KGenericContainer
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.MountableFile

@ExperimentalCoroutinesApi
@Testcontainers
class FlowScannerTest {

    companion object {
        @Container
        private val flowEmulator: KGenericContainer = KGenericContainer(
            "zolt85/flow-cli-emulator:latest"
        ).withEnv("FLOW_VERBOSE", "true").withEnv("FLOW_BLOCKTIME", "500ms")
            .withCopyFileToContainer(MountableFile.forClasspathResource("com/rarible/blockchainscanner/flow/contract"), "/home/flow-emulator/contracts")
            .withCopyFileToContainer(MountableFile.forClasspathResource("com/rarible/blockchainscanner/flow/flow.json"), "/home/flow-emulator/flow.json")
            .withExposedPorts(3569, 8080)
            .withLogConsumer {
                println(it.utf8String)
            }
            .waitingFor(Wait.forHttp("/").forPort(8080).forStatusCode(500))
    }

    @BeforeEach
    internal fun setUp() {
        flowEmulator.execInContainer("flow", "project", "deploy")
    }
}
