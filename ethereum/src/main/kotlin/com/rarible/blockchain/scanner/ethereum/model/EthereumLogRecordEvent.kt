package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.core.common.EventTimeMarks

data class EthereumLogRecordEvent(
    val record: ReversedEthereumLogRecord,
    val reverted: Boolean,
    val eventTimeMarks: EventTimeMarks? = null // TODO cleanup with other deprecated classes
)