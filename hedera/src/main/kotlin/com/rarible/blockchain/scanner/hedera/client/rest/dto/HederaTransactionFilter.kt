package com.rarible.blockchain.scanner.hedera.client.rest.dto

data class HederaTransactionFilter(
    val timestampFrom: HederaTimestampFrom? = null,
    val timestampTo: HederaTimestampTo? = null,
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

sealed class HederaTimestamp(private val prefix: String) {
    protected abstract val value: String

    fun queryValue(): String {
        return "$prefix:$value"
    }
}

sealed class HederaTimestampFrom(prefix: String) : HederaTimestamp(prefix) {
    data class Gt(override val value: String) : HederaTimestampFrom("gt")
    data class Gte(override val value: String) : HederaTimestampFrom("gte")
}

sealed class HederaTimestampTo(prefix: String) : HederaTimestamp(prefix) {
    data class Lt(override val value: String) : HederaTimestampTo("lt")
    data class Lte(override val value: String) : HederaTimestampTo("lte")
}

enum class HederaOrder(val value: String) {
    ASC("asc"),
    DESC("desc")
}

enum class HederaTransactionResult(val value: String) {
    SUCCESS("success"),
    FAIL("fail")
}
