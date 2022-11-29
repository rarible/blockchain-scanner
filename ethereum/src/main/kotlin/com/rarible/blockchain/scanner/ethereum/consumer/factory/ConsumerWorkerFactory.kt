package com.rarible.blockchain.scanner.ethereum.consumer.factory

import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecordEvent
import com.rarible.blockchain.scanner.ethereum.reduce.EntityEventListener
import com.rarible.core.daemon.sequential.ConsumerWorkerHolder

interface ConsumerWorkerFactory {
    fun create(listener: EntityEventListener): ConsumerWorkerHolder<EthereumLogRecordEvent>
}