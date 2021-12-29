package com.rarible.blockchain.scanner.monitoring

import com.rarible.blockchain.scanner.event.block.toBlock
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.configuration.TestBlockchainScannerProperties
import com.rarible.blockchain.scanner.test.data.randomBlockchainBlock
import com.rarible.core.common.nowMillis
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

@IntegrationTest
class MonitoringWorkerIt : AbstractIntegrationTest() {

    @Test
    fun `update block metrics`() {
        val blockMonitor = BlockMonitor(TestBlockchainScannerProperties(), SimpleMeterRegistry(), blockService)
        val now = nowMillis()
        val nowTimestamp = now.epochSecond
        runBlocking {
            blockService.save(randomBlockchainBlock(number = 1).copy(timestamp = nowTimestamp - 100).toBlock())
            blockService.save(randomBlockchainBlock(number = 2).copy(timestamp = nowTimestamp - 50).toBlock())
            blockService.save(randomBlockchainBlock(number = 3).copy(timestamp = nowTimestamp - 20).toBlock())
        }
        blockMonitor.refresh()
        assertThat(blockMonitor.getBlockDelay(now)).isEqualTo(20.0)
        runBlocking {
            blockService.save(randomBlockchainBlock(number = 4).copy(timestamp = nowTimestamp - 5).toBlock())
        }
        blockMonitor.refresh()
        assertThat(blockMonitor.getBlockDelay(now)).isEqualTo(5.0)
    }
}
