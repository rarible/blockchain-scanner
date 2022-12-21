package com.rarible.blockchain.scanner.ethereum.reconciliation

import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
@ExperimentalCoroutinesApi
class ReconciliationWorkerHandler(
    private val reconciliationLogHandler: ReconciliationLogHandler,

) {
    private companion object {
        val logger: Logger = LoggerFactory.getLogger(ReconciliationWorkerHandler::class.java)
    }
}
