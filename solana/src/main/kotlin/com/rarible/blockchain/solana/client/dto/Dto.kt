package com.rarible.blockchain.solana.client.dto

import com.rarible.blockchain.scanner.framework.data.TransactionMeta
import com.rarible.blockchain.solana.client.SolanaBlockchainBlock

@Suppress("unused")
abstract class Request(
    val method: String,
    val params: List<*>? = null
) {
    val jsonrpc = "2.0"
    val id = 1
}

object GetSlotRequest : Request(
    method = "getSlot"
)

class GetBlockRequest(
    slot: Long,
    transactionDetails: TransactionDetails = TransactionDetails.Full
) : Request(
    method = "getBlock",
    params = listOf(
        slot,
        mapOf(
            "transactionDetails" to transactionDetails.name.lowercase(),
            "rewards" to false
        )
    )
) {
    enum class TransactionDetails {
        Full, None
    }
}

class GetTransactionRequest(
    signature: String
) : Request(
    method = "getTransaction",
    params = listOf(signature)
)

data class ApiResponse<T>(
    val result: T
)

data class SolanaTransactionDto(
    val transaction: Details
) {
    data class Details(
        val message: Message,
        val signatures: List<String>
    )

    data class Message(
        val recentBlockhash: String
    )
}

data class SolanaBlockDto(
    val parentSlot: Long,
    val blockhash: String,
    val previousBlockhash: String?,
    val blockHeight: Long,
    val blockTime: Long
)

fun SolanaBlockDto.toModel() = SolanaBlockchainBlock(
    slot = parentSlot,
    number = blockHeight,
    hash = blockhash,
    parentHash = previousBlockhash,
    timestamp = blockTime
)

fun SolanaTransactionDto.toModel() = TransactionMeta(
    hash = transaction.signatures.first(),
    blockHash = transaction.message.recentBlockhash
)