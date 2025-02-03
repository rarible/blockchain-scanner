package com.rarible.blockchain.scanner.hedera.client.rest.dto

data class HederaBlockRequest(
    val limit: Int? = null,
    val order: HederaOrder? = null,
)
