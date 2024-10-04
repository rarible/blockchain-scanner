package com.rarible.blockchain.scanner.configuration

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.BlockchainScannerManager
import com.rarible.blockchain.scanner.block.BlockRepository
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.model.LogStorage
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.monitoring.BlockchainMonitor
import com.rarible.blockchain.scanner.monitoring.LogMonitor
import com.rarible.blockchain.scanner.monitoring.Monitor
import com.rarible.blockchain.scanner.monitoring.MonitoringWorker
import com.rarible.blockchain.scanner.monitoring.ReindexMonitor
import com.rarible.blockchain.scanner.reconciliation.LogReconciliationMonitor
import com.rarible.blockchain.scanner.reconciliation.ReconciliationLogHandler
import com.rarible.blockchain.scanner.reconciliation.ReconciliationLogHandlerImpl
import com.rarible.blockchain.scanner.reconciliation.ReconciliationLogWorkerHandler
import com.rarible.blockchain.scanner.reconciliation.ReconciliationStateRepository
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

    @Bean
    fun reconciliationLogWorkerHandler(
        reconciliationLogHandler: ReconciliationLogHandler,
        stateRepository: ReconciliationStateRepository,
        blockRepository: BlockRepository,
        scannerProperties: BlockchainScannerProperties,
        monitor: LogReconciliationMonitor,
    ): ReconciliationLogWorkerHandler {
        return ReconciliationLogWorkerHandler(
            reconciliationLogHandler,
            stateRepository,
            blockRepository,
            scannerProperties,
            monitor,
        )
    }

    @Bean
    fun reconciliationMonitor(meterRegistry: MeterRegistry): LogReconciliationMonitor {
        return LogReconciliationMonitor(properties, meterRegistry)
    }

    @Bean
    fun <BB : BlockchainBlock,
        BL : BlockchainLog,
        R : LogRecord,
        TR : TransactionRecord,
        D : Descriptor<S>,
        S : LogStorage
        > reconciliationLogHandler(
        manager: BlockchainScannerManager<BB, BL, R, TR, D, S>,
        scannerProperties: BlockchainScannerProperties,
        monitor: LogReconciliationMonitor,
        subscribers: List<LogEventSubscriber<BB, BL, R, D, S>>,
    ): ReconciliationLogHandler = ReconciliationLogHandlerImpl(
        reconciliationProperties = scannerProperties.reconciliation,
        blockchainClient = manager.retryableClient,
        logHandlerFactory = manager.logHandlerFactory,
        reindexer = manager.blockReindexer,
        planner = manager.blockScanPlanner,
        monitor = monitor,
        subscribers = subscribers,
    )
}
