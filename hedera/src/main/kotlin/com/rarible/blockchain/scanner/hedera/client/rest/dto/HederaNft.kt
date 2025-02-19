package com.rarible.blockchain.scanner.hedera.client.rest.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
@JsonIgnoreProperties(ignoreUnknown = true)
data class HederaNftResponse(
    val nfts: List<HederaNft>,
    val links: Links
)

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
@JsonIgnoreProperties(ignoreUnknown = true)
data class HederaNft(
    val accountId: String,
    val createdTimestamp: String,
    val delegatingSpender: String?,
    val deleted: Boolean,
    val metadata: String,
    val modifiedTimestamp: String,
    val serialNumber: Long,
    val spender: String?,
    val tokenId: String
)
