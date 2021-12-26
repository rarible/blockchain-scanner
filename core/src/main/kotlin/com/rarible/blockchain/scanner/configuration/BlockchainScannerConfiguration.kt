package com.rarible.blockchain.scanner.configuration

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.event.block.BlockService
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.monitoring.Monitor
import com.rarible.blockchain.scanner.monitoring.MonitoringWorker
import com.rarible.core.task.EnableRaribleTask
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.scheduling.annotation.EnableScheduling

@Configuration
@ComponentScan(basePackageClasses = [BlockchainScanner::class])
@EnableScheduling
@EnableRaribleTask
@Import(KafkaConfiguration::class)
class BlockchainScannerConfiguration(
    private val properties: BlockchainScannerProperties
) {

    @Bean
    @ConditionalOnClass(MeterRegistry::class)
    fun blockMonitor(meterRegistry: MeterRegistry, blockService: BlockService): BlockMonitor {
        return BlockMonitor(properties, meterRegistry, blockService)
    }

    @Bean
    @ConditionalOnClass(MeterRegistry::class)
    fun monitoringWorker(meterRegistry: MeterRegistry, monitors: List<Monitor>): MonitoringWorker {
        return MonitoringWorker(properties, meterRegistry, monitors)
    }
}
