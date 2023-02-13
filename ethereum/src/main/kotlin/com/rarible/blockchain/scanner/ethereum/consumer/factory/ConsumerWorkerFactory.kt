package com.rarible.blockchain.scanner.ethereum.consumer.factory

import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecordEvent
import com.rarible.blockchain.scanner.ethereum.reduce.EntityEventListener
import com.rarible.core.daemon.sequential.ConsumerWorkerHolder

@Deprecated(message = "Use from framework package", replaceWith = ReplaceWith(
    "com.rarible.blockchain.scanner.consumer.LogRecordConsumerWorkerFactory"
))
interface ConsumerWorkerFactory {
    fun create(listener: EntityEventListener): ConsumerWorkerHolder<EthereumLogRecordEvent>
}