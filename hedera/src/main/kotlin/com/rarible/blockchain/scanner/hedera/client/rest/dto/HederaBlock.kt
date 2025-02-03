package com.rarible.blockchain.scanner.hedera.client.rest.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
@JsonIgnoreProperties(ignoreUnknown = true)
data class HederaBlocksResponse(
    val blocks: List<HederaBlock>,
    val links: Links
)

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
@JsonIgnoreProperties(ignoreUnknown = true)
data class HederaBlock(
    val count: Long,
    val hapiVersion: String,
    val hash: String,
    val name: String,
    val number: Long,
    val previousHash: String,
    val size: Long,
    val timestamp: Timestamp,
    val gasUsed: Long,
    val logsBloom: String
)

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
@JsonIgnoreProperties(ignoreUnknown = true)
data class Timestamp(
    val from: String,
    val to: String
)
