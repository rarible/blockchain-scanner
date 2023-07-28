package com.rarible.blockchain.scanner.reindex

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.subscriber.LogEventFilter
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.framework.subscriber.LogRecordComparator
import com.rarible.blockchain.scanner.handler.LogHandler
import com.rarible.blockchain.scanner.monitoring.LogMonitor
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher

class LogHandlerFactory<BB : BlockchainBlock, BL : BlockchainLog, R : LogRecord, D : Descriptor>(
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    private val logService: LogService<R, D>,
    private val logFilters: List<LogEventFilter<R, D>>,
    private val logRecordComparator: LogRecordComparator<R>,
    private val logMonitor: LogMonitor
) {

    fun create(
        groupId: String,
        subscribers: List<LogEventSubscriber<BB, BL, R, D>>,
        logRecordEventPublisher: LogRecordEventPublisher,
    ) = create(groupId, subscribers, logService, logRecordEventPublisher)

    fun create(
        groupId: String,
        subscribers: List<LogEventSubscriber<BB, BL, R, D>>,
        logService: LogService<R, D>,
        logRecordEventPublisher: LogRecordEventPublisher,
    ) = LogHandler(
        groupId = groupId,
        blockchainClient = blockchainClient,
        subscribers = subscribers,
        logFilters = logFilters,
        logService = logService,
        logRecordComparator = logRecordComparator,
        logRecordEventPublisher = logRecordEventPublisher,
        logMonitor = logMonitor
    )
}
