package com.rarible.blockchain.scanner.configuration

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.block.BlockRepository
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.monitoring.BlockchainMonitor
import com.rarible.blockchain.scanner.monitoring.LogMonitor
import com.rarible.blockchain.scanner.monitoring.Monitor
import com.rarible.blockchain.scanner.monitoring.MonitoringWorker
import com.rarible.blockchain.scanner.monitoring.ReindexMonitor
import com.rarible.blockchain.scanner.reconciliation.ReconciliationLogWorkerHandler
import com.rarible.core.daemon.DaemonWorkerProperties
import com.rarible.core.daemon.job.JobDaemonWorker
import com.rarible.core.daemon.sequential.SequentialDaemonWorker
import com.rarible.core.task.EnableRaribleTask
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import

@Configuration
@EnableRaribleTask
@ComponentScan(basePackageClasses = [BlockchainScanner::class, ReconciliationLogWorkerHandler::class])
@Import(KafkaConfiguration::class)
class BlockchainScannerConfiguration(
    private val properties: BlockchainScannerProperties
) {

    @Bean
    fun blockMonitor(
        meterRegistry: MeterRegistry,
        blockRepository: BlockRepository
    ): BlockMonitor {
        return BlockMonitor(blockRepository, properties, meterRegistry)
    }

    @Bean
    fun logMonitor(meterRegistry: MeterRegistry): LogMonitor {
        return LogMonitor(properties, meterRegistry)
    }

    @Bean
    fun blockchainMonitor(meterRegistry: MeterRegistry): BlockchainMonitor {
        return BlockchainMonitor(meterRegistry)
    }

    @Bean
    fun reindexMonitor(meterRegistry: MeterRegistry): ReindexMonitor {
        return ReindexMonitor(properties, meterRegistry)
    }

    @Bean
    fun monitoringWorker(meterRegistry: MeterRegistry, monitors: List<Monitor>): SequentialDaemonWorker =
        MonitoringWorker(properties, meterRegistry, monitors)

    @Bean
    fun blockchainScannerReconciliationLogHandlerWorker(handler: ReconciliationLogWorkerHandler, meterRegistry: MeterRegistry): JobDaemonWorker {
        return JobDaemonWorker(
            jobHandler = handler,
            meterRegistry = meterRegistry,
            properties = DaemonWorkerProperties(
                pollingPeriod = properties.reconciliation.checkPeriod,
            ),
            workerName = "blockchain-scanner-reconciliation-log-handler-worker"
        ).apply {
            if (properties.reconciliation.enabled) {
                start()
            }
        }
    }
}
