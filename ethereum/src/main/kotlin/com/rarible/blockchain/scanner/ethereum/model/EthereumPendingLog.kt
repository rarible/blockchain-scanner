package com.rarible.blockchain.scanner.ethereum.model

class EthereumPendingLog(
    val record: EthereumLogRecord<*>,
    val descriptor: EthereumDescriptor
)
