package com.rarible.blockchain.scanner.monitoring

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.core.daemon.sequential.SequentialDaemonWorker
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.time.delay
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener

class MonitoringWorker(
    private val properties: BlockchainScannerProperties,
    meterRegistry: MeterRegistry,
    private val monitors: List<Monitor>
) : SequentialDaemonWorker(meterRegistry, properties.monitoring.worker) {

    @EventListener(ApplicationReadyEvent::class)
    fun onApplicationStarted() {
        if (!properties.scan.enabled) {
            logger.info("Blockchain scanning for '{}' is disabled", properties.blockchain)
            return
        }
        if (!properties.monitoring.enabled) {
            logger.info(
                "Monitoring of BlockchainScanner for '{}' disabled, no metrics will be available",
                properties.blockchain
            )
            return
        }
        logger.info(
            "Starting monitoring BlockchainScanner for '{}' with {} monitors: {}",
            properties.blockchain,
            monitors.size,
            monitors
        )
        monitors.forEach { it.register() }
        start()
    }

    override suspend fun handle() {
        monitors.forEach {
            try {
                it.refresh()
            } catch (e: Exception) {
                logger.error("Unable to update metrics for monitor [{}]", it, e)
            }
        }
        logger.debug("All monitors updated, next iteration starts in {}", pollingPeriod)
        delay(pollingPeriod)
    }
}
