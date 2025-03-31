package com.rarible.blockchain.scanner.hedera.client.rest.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming

data class HederaContractResultsRequest(
    val from: String? = null,
    val blockHash: String? = null,
    val blockNumber: String? = null,
    val internal: Boolean = false,
    val limit: Int = 25,
    val order: HederaOrder = HederaOrder.DESC,
    val timestamp: List<String>? = null,
    val transactionIndex: Int? = null
)

data class HederaContractResultsResponse(
    val results: List<HederaContractResult> = emptyList(),
    val links: Links
)

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
@JsonIgnoreProperties(ignoreUnknown = true)
data class HederaContractResult(
    val address: String?,
    val accessList: String?,
    val amount: Long?,
    val blockGasUsed: Long?,
    val blockHash: String?,
    val blockNumber: Long?,
    val bloom: String?,
    val callResult: String?,
    val chainId: String?,
    val contractId: String?,
    val createdContractIds: List<String>?,
    val errorMessage: String?,
    val failedInitcode: String?,
    val from: String?,
    val functionParameters: String?,
    val gasConsumed: Long?,
    val gasLimit: Long?,
    val gasPrice: String?,
    val gasUsed: Long?,
    val hash: String?,
    val maxFeePerGas: String?,
    val maxPriorityFeePerGas: String?,
    val nonce: Long?,
    val r: String?,
    val result: String?,
    val s: String?,
    val status: String?,
    val timestamp: String?,
    val to: String?,
    val transactionIndex: Long?,
    val type: Int?,
    val v: Int?
)
