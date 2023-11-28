package com.rarible.blockchain.scanner.solana

import com.rarible.blockchain.scanner.BlockchainScannerManager
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.monitoring.LogMonitor
import com.rarible.blockchain.scanner.monitoring.ReindexMonitor
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.publisher.TransactionRecordEventPublisher
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainBlock
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainLog
import com.rarible.blockchain.scanner.solana.client.SolanaClient
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord
import com.rarible.blockchain.scanner.solana.service.SolanaLogService
import com.rarible.blockchain.scanner.solana.subscriber.SolanaLogEventSubscriber
import com.rarible.blockchain.scanner.solana.subscriber.SolanaLogEventSubscriberExceptionResolver
import com.rarible.blockchain.scanner.solana.subscriber.SolanaLogRecordComparator
import org.springframework.stereotype.Component

@Component
class SolanaBlockchainScannerManager(
    blockchainClient: SolanaClient,
    subscribers: List<SolanaLogEventSubscriber>,
    blockService: BlockService,
    logService: SolanaLogService,
    properties: BlockchainScannerProperties,
    logEventPublisher: LogRecordEventPublisher,
    blockMonitor: BlockMonitor,
    logMonitor: LogMonitor,
    reindexMonitor: ReindexMonitor,
    transactionRecordEventPublisher: TransactionRecordEventPublisher,
    logEventSubscriberExceptionResolver: SolanaLogEventSubscriberExceptionResolver,
) : BlockchainScannerManager<SolanaBlockchainBlock, SolanaBlockchainLog, SolanaLogRecord, TransactionRecord, SolanaDescriptor>(
    blockchainClient = blockchainClient,
    logSubscribers = subscribers,
    blockService = blockService,
    logService = logService,
    logRecordComparator = SolanaLogRecordComparator,
    properties = properties,
    logRecordEventPublisher = logEventPublisher,
    blockMonitor = blockMonitor,
    logMonitor = logMonitor,
    reindexMonitor = reindexMonitor,
    transactionRecordEventPublisher = transactionRecordEventPublisher,
    transactionSubscribers = emptyList(),
    logEventSubscriberExceptionResolver = logEventSubscriberExceptionResolver,
)
