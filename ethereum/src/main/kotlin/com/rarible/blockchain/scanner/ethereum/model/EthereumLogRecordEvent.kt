package com.rarible.blockchain.scanner.ethereum.model

data class EthereumLogRecordEvent(
    val record: ReversedEthereumLogRecord,
    val reverted: Boolean
)