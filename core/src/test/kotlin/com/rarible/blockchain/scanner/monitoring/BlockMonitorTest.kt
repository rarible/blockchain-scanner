package com.rarible.blockchain.scanner.monitoring

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.block.BlockRepository
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.configuration.MonitoringProperties
import io.micrometer.core.instrument.MeterRegistry
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class BlockMonitorTest {
    private val blockRepository = mockk<BlockRepository> {
        coEvery { failedCount() } returns 0
    }
    private val properties = mockk<BlockchainScannerProperties> {
        every { blockchain } returns "ethereum"
        every { monitoring } returns MonitoringProperties()
    }
    private val meterRegistry = mockk<MeterRegistry>()

    private val blockMonitor = BlockMonitor(
        blockRepository,
        properties,
        meterRegistry
    )

    @Test
    fun `set init lastIndexedBlock - ok`() = runBlocking<Unit> {
        val block = mockk<Block>()

        coEvery { blockRepository.getLastBlock() } returns block

        blockMonitor.refresh()

        assertThat(blockMonitor.getLastIndexedBlock()).isEqualTo(block)
    }

    @Test
    fun `set init lastIndexedBlock - skip`() = runBlocking<Unit> {
        val recordBlock = mockk<Block>()
        val savedBlock = mockk<Block>()

        coEvery { blockRepository.getLastBlock() } returns savedBlock

        blockMonitor.recordLastIndexedBlock(recordBlock)
        blockMonitor.refresh()

        assertThat(blockMonitor.getLastIndexedBlock()).isEqualTo(recordBlock)
    }
}
