package com.rarible.blockchain.scanner.test.reindex

import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.reindex.SubscriberFilter
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLogRecord

class TestSubscriberFilter(
    private val collections: Set<String>
) : SubscriberFilter<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestDescriptor> {

    override fun filter(
        all: List<LogEventSubscriber<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestDescriptor>>
    ): List<LogEventSubscriber<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestDescriptor>> {
        return all.filter { collections.contains(it.getDescriptor().collection) }
    }
}