package com.rarible.blockchain.scanner.data

import com.fasterxml.jackson.annotation.JsonIgnore

data class NewBlockEvent(val number: Long, val hash: String, val timestamp: Long, val reverted: String? = null) {
    @get:JsonIgnore
    val contextParams: Map<String, String>
        get() = mapOf(
            "blockNumber" to number.toString(),
            "blockHash" to hash,
            "eventType" to "newBlock"
        ) + if (reverted != null) mapOf("reverted" to reverted) else emptyMap()
}