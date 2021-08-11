package com.rarible.blockchain.scanner.monitoring

import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.randomOriginalBlock
import com.rarible.core.test.wait.BlockingWait
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired

@IntegrationTest
class MonitoringWorkerIt : AbstractIntegrationTest() {

    @Autowired
    lateinit var blockMonitor: BlockMonitor

    @Autowired
    lateinit var registry: MeterRegistry

    @Test
    fun `update block metrics`() = runBlocking {
        saveBlock(randomOriginalBlock(), status = Block.Status.PENDING)
        saveBlock(randomOriginalBlock(), status = Block.Status.ERROR)
        saveBlock(randomOriginalBlock(), status = Block.Status.ERROR)
        saveBlock(randomOriginalBlock(), status = Block.Status.SUCCESS)

        BlockingWait.waitAssert {
            assertEquals(2, blockMonitor.getErrorBlockCount().toLong())
            assertEquals(1, blockMonitor.getPendingBlockCount().toLong())
            assertTrue(blockMonitor.getBlockDelay()!!.toLong() > 0)
            assertEquals(2, registry.find("blockchain.scanner.block.error").gauge()!!.value().toLong())
            assertEquals(1, registry.find("blockchain.scanner.block.pending").gauge()!!.value().toLong())
            assertTrue(registry.find("blockchain.scanner.block.delay").gauge()!!.value().toLong() > 0)
        }
    }


}