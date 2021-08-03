package com.rarible.blockchain.scanner.test.service

import com.rarible.blockchain.scanner.data.LogEvent
import com.rarible.blockchain.scanner.data.LogEventStatusUpdate
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow

class TestPendingLogService : PendingLogService<TestBlockchainBlock, TestLog, TestDescriptor> {

    override fun markInactive(
        block: TestBlockchainBlock,
        logs: List<LogEvent<TestLog, TestDescriptor>>
    ): Flow<LogEventStatusUpdate<TestLog, TestDescriptor>> {
        return emptyFlow()
    }
}