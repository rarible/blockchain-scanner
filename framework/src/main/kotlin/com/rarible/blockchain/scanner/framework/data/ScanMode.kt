package com.rarible.blockchain.scanner.framework.data

enum class ScanMode(val eventSource: String) {
    REALTIME("blockchain"),
    REINDEX("reindex")
}