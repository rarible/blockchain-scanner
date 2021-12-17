package com.rarible.blockchain.scanner.solana

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.consumer.BlockEventConsumer
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.publisher.LogEventPublisher
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainBlock
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainLog
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import com.rarible.blockchain.scanner.solana.client.SolanaRetryableClient
import com.rarible.blockchain.scanner.solana.mapper.SolanaBlockMapper
import com.rarible.blockchain.scanner.solana.mapper.SolanaLogMapper
import com.rarible.blockchain.scanner.solana.model.SolanaBlock
import com.rarible.blockchain.scanner.solana.model.SolanaLog
import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord
import com.rarible.blockchain.scanner.solana.service.SolanaBlockService
import com.rarible.blockchain.scanner.solana.service.SolanaLogService
import com.rarible.blockchain.scanner.solana.subscriber.SolanaLogEventComparator
import com.rarible.blockchain.scanner.solana.subscriber.SolanaLogEventSubscriber
import kotlinx.coroutines.reactor.mono
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

@Component
class SolanaBlockchainScanner(
    blockchainClient: SolanaRetryableClient,
    subscribers: List<SolanaLogEventSubscriber>,
    blockMapper: SolanaBlockMapper,
    blockService: SolanaBlockService,
    logMapper: SolanaLogMapper,
    logService: SolanaLogService,
    properties: BlockchainScannerProperties,
    // Autowired from core
    blockEventPublisher: BlockEventPublisher,
    blockEventConsumer: BlockEventConsumer,
    logEventComparator: SolanaLogEventComparator,
    logEventPublisher: LogEventPublisher
) : BlockchainScanner<SolanaBlockchainBlock, SolanaBlockchainLog, SolanaBlock, SolanaLog, SolanaLogRecord<*>, SolanaDescriptor>(
    blockchainClient,
    subscribers,
    blockMapper,
    blockService,
    logMapper,
    logService,
    logEventComparator,
    properties,
    blockEventPublisher,
    blockEventConsumer,
    logEventPublisher
) {

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        mono { (scan()) }.subscribe()
    }
}
