package com.rarible.blockchain.scanner.model

class RichLogEvent<L : LogEvent>(
    val log: L,
    val collection: String
)