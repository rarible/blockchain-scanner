package com.rarible.blockchain.scanner.configuration

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.consumer.BlockEventConsumer
import com.rarible.blockchain.scanner.consumer.DefaultBlockEventConsumer
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.monitoring.Monitor
import com.rarible.blockchain.scanner.monitoring.MonitoringWorker
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.publisher.DefaultBlockEventPublisher
import com.rarible.core.task.EnableRaribleTask
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.scheduling.annotation.EnableScheduling


@Configuration
@ComponentScan(basePackageClasses = [BlockchainScanner::class])
@EnableScheduling
@EnableRaribleTask
@FlowPreview
@ExperimentalCoroutinesApi
@Import(KafkaConfiguration::class)
class BlockchainScannerConfiguration(
    private val properties: BlockchainScannerProperties
) {

    @Bean
    @ConditionalOnBean(MeterRegistry::class)
    fun blockMonitor(meterRegistry: MeterRegistry, blockService: BlockService<*>): BlockMonitor {
        return BlockMonitor(properties, meterRegistry, blockService)
    }

    @Bean
    @ConditionalOnBean(MeterRegistry::class)
    fun monitoringWorker(meterRegistry: MeterRegistry, monitors: List<Monitor>): MonitoringWorker {
        return MonitoringWorker(properties, meterRegistry, monitors)
    }

    @Bean
    @ConditionalOnMissingBean(BlockEventConsumer::class)
    fun defaultBlockEventConsumer(): DefaultBlockEventConsumer {
        return DefaultBlockEventConsumer()
    }

    @Bean
    @ConditionalOnMissingBean(BlockEventPublisher::class)
    fun defaultBlockEventPublisher(defaultBlockEventConsumer: DefaultBlockEventConsumer): DefaultBlockEventPublisher {
        return DefaultBlockEventPublisher(defaultBlockEventConsumer)
    }

}
