package com.rarible.blockchain.scanner.subscriber

data class LogEventDescriptor(
    val collection: String,
    val topic: String,
    val contracts: Collection<String>
) {
}