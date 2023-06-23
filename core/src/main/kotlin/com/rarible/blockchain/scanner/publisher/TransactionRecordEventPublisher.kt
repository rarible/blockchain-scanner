package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.framework.data.TransactionRecordEvent
import com.rarible.blockchain.scanner.framework.model.TransactionRecord

interface TransactionRecordEventPublisher : RecordEventPublisher<TransactionRecord, TransactionRecordEvent>
