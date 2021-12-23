package com.rarible.blockchain.scanner.solana.client.dto

import com.rarible.blockchain.scanner.solana.client.SolanaBlockEvent
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainBlock

@Suppress("unused")
abstract class Request(
    val method: String,
    val params: List<*>? = null
) {
    val jsonrpc = "2.0"
    val id = 1
}

object GetSlotRequest : Request(
    method = "getSlot",
    params = listOf(
        mapOf("commitment" to "confirmed")
    )
)

class GetBlockRequest(
    slot: Long,
    transactionDetails: TransactionDetails
) : Request(
    method = "getBlock",
    params = listOf(
        slot,
        mapOf(
            "encoding" to "json",
            "transactionDetails" to transactionDetails.name.lowercase(),
            "commitment" to "confirmed",
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
    params = listOf(
        signature,
        mapOf("commitment" to "confirmed")
    )
)

data class ApiResponse<T>(
    val result: T?,
    val error: Error?
) {
    data class Error(
        val message: String,
        val code: Int
    )
}

data class SolanaTransactionDto(
    val transaction: Details,
    val meta: Meta,
) {
    data class Instruction(
        val accounts: List<Int>,
        val data: String,
        val programIdIndex: Int
    )

    data class InnerInstruction(
        val index: Int,
        val instructions: List<Instruction>,
    )

    data class Message(
        val recentBlockhash: String?,
        val accountKeys: List<String>,
        val instructions: List<Instruction>
    )

    data class Details(
        val message: Message,
        val signatures: List<String>
    )

    data class Meta(
        val innerInstructions: List<InnerInstruction>
    )
}

data class SolanaBlockDto(
    val parentSlot: Long,
    val blockhash: String,
    val previousBlockhash: String?,
    val blockHeight: Long,
    val blockTime: Long,
    val transactions: List<SolanaTransactionDto> = emptyList()
)

fun ApiResponse<Long>.toModel(): Long = result!!

fun ApiResponse<SolanaBlockDto>.toModel(slot: Long): SolanaBlockchainBlock? {
    if (result != null) {
        return with(result) {
            SolanaBlockchainBlock(
                slot = slot,
                parentSlot = parentSlot,
                number = blockHeight,
                hash = blockhash,
                parentHash = previousBlockhash,
                timestamp = blockTime
            )
        }
    }

    val code = requireNotNull(error) { "error field must be not null" }.code
    if (code == ErrorCodes.BLOCK_NOT_AVAILABLE) {
        return null
    } else {
        error("Unknown error code: $code")
    }
}

fun SolanaTransactionDto.toModel(): List<SolanaBlockEvent> {
    val instructions = transaction.message.instructions + meta.innerInstructions.flatMap { it.instructions }
    val accountKeys = transaction.message.accountKeys

    return instructions.map { transaction ->
        val programId = accountKeys[transaction.programIdIndex]
        val data = transaction.data
        val accounts = transaction.accounts.map { accountKeys[it] }

        SolanaBlockEvent(programId, data, accounts)
    }
}

object ErrorCodes {
    const val BLOCK_NOT_AVAILABLE = -32004
}