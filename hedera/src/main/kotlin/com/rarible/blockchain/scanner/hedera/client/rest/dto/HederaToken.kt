package com.rarible.blockchain.scanner.hedera.client.rest.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
@JsonIgnoreProperties(ignoreUnknown = true)
data class HederaToken(
    val autoRenewAccount: String,
    val autoRenewPeriod: Long,
    val createdTimestamp: String,
    val customFees: CustomFees,
    val decimals: String,
    val deleted: Boolean,
    val expiryTimestamp: Long,
    val feeScheduleKey: String?,
    val freezeDefault: Boolean,
    val freezeKey: String?,
    val initialSupply: String,
    val kycKey: String?,
    val maxSupply: String,
    val memo: String,
    val metadata: String,
    val metadataKey: String?,
    val modifiedTimestamp: String,
    val name: String,
    val pauseKey: String?,
    val pauseStatus: String,
    val supplyType: String,
    val symbol: String,
    val tokenId: String,
    val totalSupply: String,
    val treasuryAccountId: String,
    val type: String,
    val wipeKey: String?
)

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
@JsonIgnoreProperties(ignoreUnknown = true)
data class CustomFees(
    val createdTimestamp: String,
)
