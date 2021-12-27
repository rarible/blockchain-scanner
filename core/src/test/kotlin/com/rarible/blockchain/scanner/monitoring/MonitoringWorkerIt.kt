package com.rarible.blockchain.scanner.monitoring

import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.randomBlockchainBlock
import com.rarible.core.test.wait.BlockingWait
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import

@IntegrationTest
@Import(MonitoringWorkerIt.TestConfiguration::class)
class MonitoringWorkerIt : AbstractIntegrationTest() {

    @Autowired
    lateinit var blockMonitor: BlockMonitor

    @Autowired
    lateinit var registry: MeterRegistry

    @Test
    fun `update block metrics`() = runBlocking {
        saveBlock(randomBlockchainBlock())
        saveBlock(randomBlockchainBlock())
        saveBlock(randomBlockchainBlock())
        saveBlock(randomBlockchainBlock())

        BlockingWait.waitAssert {
            assertTrue(blockMonitor.getBlockDelay()!!.toLong() > 0)
            assertTrue(registry.find("blockchain.scanner.block.delay").gauge()!!.value().toLong() > 0)
        }
    }

    @org.springframework.boot.test.context.TestConfiguration
    class TestConfiguration {
        @Bean
        fun simpleMeterRegistry(): MeterRegistry  {
            return SimpleMeterRegistry()
        }
    }
}
