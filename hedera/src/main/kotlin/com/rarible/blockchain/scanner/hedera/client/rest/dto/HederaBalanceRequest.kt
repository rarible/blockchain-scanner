package com.rarible.blockchain.scanner.hedera.client.rest.dto

data class HederaBalanceRequest(
    val accountId: String? = null,
    val limit: Int? = null,
    val order: HederaOrder? = null
)
