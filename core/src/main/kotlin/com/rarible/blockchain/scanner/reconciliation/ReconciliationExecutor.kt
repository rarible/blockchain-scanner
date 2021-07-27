package com.rarible.blockchain.scanner.reconciliation

import reactor.core.publisher.Flux

interface ReconciliationExecutor {

    fun reconcile(topic: String?, from: Long): Flux<LongRange>

    fun getTopics(): Set<String>

}