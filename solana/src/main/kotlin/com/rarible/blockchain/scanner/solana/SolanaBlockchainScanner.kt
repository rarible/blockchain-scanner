package com.rarible.blockchain.scanner.solana

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainBlock
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainLog
import com.rarible.blockchain.scanner.solana.client.SolanaClient
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord
import com.rarible.blockchain.scanner.solana.service.SolanaLogService
import com.rarible.blockchain.scanner.solana.subscriber.SolanaLogEventFilter
import com.rarible.blockchain.scanner.solana.subscriber.SolanaLogEventSubscriber
import com.rarible.blockchain.scanner.solana.subscriber.SolanaLogRecordComparator
import kotlinx.coroutines.reactor.mono
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

@Component
class SolanaBlockchainScanner(
    blockchainClient: SolanaClient,
    subscribers: List<SolanaLogEventSubscriber>,
    logFilters: List<SolanaLogEventFilter>,
    blockService: BlockService,
    logService: SolanaLogService,
    properties: BlockchainScannerProperties,
    logEventPublisher: LogRecordEventPublisher,
    monitor: BlockMonitor
) : BlockchainScanner<SolanaBlockchainBlock, SolanaBlockchainLog, SolanaLogRecord, SolanaDescriptor>(
    blockchainClient,
    subscribers,
    logFilters,
    blockService,
    logService,
    SolanaLogRecordComparator,
    properties,
    logEventPublisher,
    monitor
) {

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        mono { (scan()) }.subscribe()
    }
}
