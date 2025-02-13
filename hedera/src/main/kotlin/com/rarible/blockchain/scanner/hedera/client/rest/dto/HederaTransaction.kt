package com.rarible.blockchain.scanner.hedera.client.rest.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming
import java.math.BigInteger

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
@JsonIgnoreProperties(ignoreUnknown = true)
data class HederaTransactionsResponse(
    val transactions: List<HederaTransaction>,
    val links: Links
)

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
@JsonIgnoreProperties(ignoreUnknown = true)
data class HederaTransaction(
    val bytes: String?,
    val chargedTxFee: Long,
    val consensusTimestamp: String,
    val entityId: String?,
    val maxFee: String,
    val memoBase64: String?,
    val name: String,
    val node: String?,
    val nonce: Int,
    val parentConsensusTimestamp: String?,
    val result: String,
    val scheduled: Boolean,
    val transactionHash: String,
    val transactionId: String,
    val transfers: List<Transfer>,
    val nftTransfers: List<NftTransfer>?,
    val validDurationSeconds: String?,
    val validStartTimestamp: String
)

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
@JsonIgnoreProperties(ignoreUnknown = true)
data class Transfer(
    val account: String,
    val amount: Long,
    val isApproval: Boolean
)

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
@JsonIgnoreProperties(ignoreUnknown = true)
data class NftTransfer(
    val isApproval: Boolean,
    val receiverAccountId: String?,
    val senderAccountId: String?,
    val serialNumber: BigInteger,
    val tokenId: String
)
