package com.rarible.blockchain.scanner.reindex

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.model.LogStorage
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriberExceptionResolver
import com.rarible.blockchain.scanner.framework.subscriber.LogRecordComparator
import com.rarible.blockchain.scanner.handler.LogHandler
import com.rarible.blockchain.scanner.monitoring.LogMonitor
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher

class LogHandlerFactory<
    BB : BlockchainBlock,
    BL : BlockchainLog,
    R : LogRecord,
    D : Descriptor<S>,
    S : LogStorage
    >(
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    private val logService: LogService<R, D, S>,
    private val logRecordComparator: LogRecordComparator<R>,
    private val logEventSubscriberExceptionResolver: LogEventSubscriberExceptionResolver,
    private val logMonitor: LogMonitor
) {

    fun create(
        groupId: String,
        subscribers: List<LogEventSubscriber<BB, BL, R, D, S>>,
        logRecordEventPublisher: LogRecordEventPublisher,
        readOnly: Boolean = false,
    ) = LogHandler(
        groupId = groupId,
        blockchainClient = blockchainClient,
        subscribers = subscribers,
        logService = logService,
        logRecordComparator = logRecordComparator,
        logRecordEventPublisher = logRecordEventPublisher,
        logMonitor = logMonitor,
        logEventSubscriberExceptionResolver = logEventSubscriberExceptionResolver,
        readOnly = readOnly,
    )
}
