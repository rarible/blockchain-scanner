package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.framework.data.ScannerEventTimeMarks

data class EthereumLogRecordEvent(
    val record: ReversedEthereumLogRecord,
    val reverted: Boolean,
    val eventTimeMarks: ScannerEventTimeMarks? = null // TODO cleanup with other deprecated classes
)