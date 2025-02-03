package com.rarible.blockchain.scanner.hedera.client.rest.dto

data class HederaTransactionFilter(
    val accountId: String? = null,
    val timestamp: String? = null,
    val limit: Int? = null,
    val order: HederaOrder? = null,
    val transactionType: HederaTransactionType? = null,
    val result: HederaTransactionResult? = null
) {
    companion object {
        const val DEFAULT_LIMIT = 25
        const val MAX_LIMIT = 100
    }
}

enum class HederaOrder(val value: String) {
    ASC("asc"),
    DESC("desc")
}

enum class HederaTransactionResult(val value: String) {
    SUCCESS("success"),
    FAIL("fail")
}
