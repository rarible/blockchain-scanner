package com.rarible.blockchain.scanner.hedera.client.rest.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
@JsonIgnoreProperties(ignoreUnknown = true)
data class HederaToken(
    val autoRenewAccount: String?,
    val autoRenewPeriod: Long,
    val createdTimestamp: String,
    val customFees: CustomFees,
    val decimals: String,
    val deleted: Boolean,
    val expiryTimestamp: Long,
    val freezeDefault: Boolean,
    val initialSupply: String,
    val maxSupply: String,
    val memo: String,
    val metadata: String,
    val modifiedTimestamp: String,
    val name: String,
    val pauseStatus: String,
    val supplyType: String,
    val symbol: String,
    val tokenId: String,
    val totalSupply: String,
    val treasuryAccountId: String,
    val type: String,
)

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
@JsonIgnoreProperties(ignoreUnknown = true)
data class CustomFees(
    val createdTimestamp: String,
    val royaltyFees: List<CustomRoyaltyFee>?,
)

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
@JsonIgnoreProperties(ignoreUnknown = true)
data class CustomRoyaltyFee(
    val collectorAccountId: String,
    val amount: Fee,
    val fallbackFee: FallbackFee?
)

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
@JsonIgnoreProperties(ignoreUnknown = true)
data class Fee(
    val numerator: Long,
    val denominator: Long,
)

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
@JsonIgnoreProperties(ignoreUnknown = true)
data class FallbackFee(
    val amount: Long, // HBAR * 1e-8
    val denominatingTokenId: String?,
)
