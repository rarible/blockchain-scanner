package com.rarible.blockchain.scanner.ethereum.subscriber

import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.subscriber.LogEventListener

interface EthereumLogEventListener : LogEventListener<EthereumLog, EthereumLogRecord<*>> {
}