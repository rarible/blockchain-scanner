package com.rarible.blockchain.scanner.hedera.model

import com.rarible.blockchain.scanner.framework.model.Descriptor

abstract class HederaDescriptor(
    val filter: HederaTransactionFilter,
    override val id: String,
    override val groupId: String,
    override val storage: HederaLogStorage
) : Descriptor<HederaLogStorage>

abstract class TransactionTypeHederaDescriptor(
    transactionType: String,
    id: String,
    groupId: String,
    storage: HederaLogStorage
) : HederaDescriptor(
    filter = HederaTransactionFilter.ByTransactionType(transactionType),
    id = id,
    groupId = groupId,
    storage = storage
)
