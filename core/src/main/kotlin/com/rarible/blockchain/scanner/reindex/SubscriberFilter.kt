package com.rarible.blockchain.scanner.reindex

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.model.LogStorage
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber

interface SubscriberFilter<
    BB : BlockchainBlock,
    BL : BlockchainLog,
    R : LogRecord,
    D : Descriptor<S>,
    S : LogStorage
    > {

    fun filter(all: List<LogEventSubscriber<BB, BL, R, D, S>>): List<LogEventSubscriber<BB, BL, R, D, S>>
}
