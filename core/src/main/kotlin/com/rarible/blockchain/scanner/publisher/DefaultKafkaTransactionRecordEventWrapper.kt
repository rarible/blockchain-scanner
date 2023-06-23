package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.framework.data.TransactionRecordEvent

class DefaultKafkaTransactionRecordEventWrapper : KafkaTransactionRecordEventWrapper<TransactionRecordEvent> {
    override val targetClass: Class<TransactionRecordEvent>
        get() = TransactionRecordEvent::class.java

    override fun wrap(transactionRecordEvent: TransactionRecordEvent): TransactionRecordEvent = transactionRecordEvent
}