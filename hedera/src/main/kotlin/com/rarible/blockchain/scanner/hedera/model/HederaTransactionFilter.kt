package com.rarible.blockchain.scanner.hedera.model

import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransaction

interface HederaTransactionFilter {
    fun matches(transaction: HederaTransaction): Boolean

    data class ByEntityId(val entityId: String) : HederaTransactionFilter {
        override fun matches(transaction: HederaTransaction): Boolean {
            return transaction.entityId == entityId
        }
    }

    data class ByTransactionType(val transactionType: String) : HederaTransactionFilter {
        override fun matches(transaction: HederaTransaction): Boolean {
            return transaction.name == transactionType
        }
    }

    data class And(val filters: Collection<HederaTransactionFilter>) : HederaTransactionFilter {
        constructor(vararg filters: HederaTransactionFilter) : this(filters.toSet())

        override fun matches(transaction: HederaTransaction): Boolean {
            return filters.all { it.matches(transaction) }
        }
    }

    data class Or(val filters: Collection<HederaTransactionFilter>) : HederaTransactionFilter {
        constructor(vararg filters: HederaTransactionFilter) : this(filters.toSet())

        override fun matches(transaction: HederaTransaction): Boolean =
            filters.any { it.matches(transaction) }
    }

    object True : HederaTransactionFilter {
        override fun matches(transaction: HederaTransaction): Boolean = true
    }
}
