package com.rarible.blockchain.scanner.test.reindex

import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.reindex.SubscriberFilter
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.repository.TestLogStorage

class TestSubscriberFilter(
    private val storages: Set<TestLogStorage>
) : SubscriberFilter<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestDescriptor, TestLogStorage> {

    override fun filter(
        all: List<LogEventSubscriber<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestDescriptor, TestLogStorage>>
    ): List<LogEventSubscriber<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestDescriptor, TestLogStorage>> {
        return all.filter { storages.contains(it.getDescriptor().storage) }
    }
}
