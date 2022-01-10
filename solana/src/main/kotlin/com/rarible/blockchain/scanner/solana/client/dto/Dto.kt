package com.rarible.blockchain.scanner.solana.client.dto

import com.rarible.blockchain.scanner.solana.client.SolanaInstruction
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainBlock
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainLog
import com.rarible.blockchain.scanner.solana.client.dto.SolanaTransactionDto.Instruction
import com.rarible.blockchain.scanner.solana.model.SolanaLog

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

object GetFirstAvailableBlockRequest : Request(
    method = "getFirstAvailableBlock"
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
    val meta: Meta?,
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
) {
    companion object {
        val errorsToSkip = listOf(ErrorCodes.BLOCK_NOT_AVAILABLE, ErrorCodes.SLOT_WAS_SKIPPED)
    }
}

fun ApiResponse<Long>.toModel(): Long = convert { this }

fun ApiResponse<SolanaBlockDto>.toModel(slot: Long): SolanaBlockchainBlock? = convert(SolanaBlockDto.errorsToSkip) {
    val logs = transactions.flatMapIndexed { transactionIndex, transactionDto ->
        val transaction = transactionDto.transaction
        val accountKeys = transaction.message.accountKeys
        val transactionHash = transactionDto.transaction.signatures.first()
        val result = arrayListOf<SolanaBlockchainLog>().apply {
            this += transaction.message.instructions.map {
                it.toModel(
                    accountKeys,
                    slot,
                    blockhash,
                    transactionHash,
                    transactionIndex,
                    innerTransactionIndex = null
                )
            }

            transactionDto.meta?.let { meta ->
                this += meta.innerInstructions.flatMapIndexed { innerInstructionIndex, innerInstruction ->
                    innerInstruction.instructions.map { instruction ->
                        instruction.toModel(
                            accountKeys,
                            slot,
                            blockhash,
                            transactionHash,
                            innerInstruction.index,
                            innerInstructionIndex
                        )
                    }
                }
            }
        }

        result
    }

    SolanaBlockchainBlock(
        slot = slot,
        parentSlot = parentSlot,
        logs = logs,
        hash = blockhash,
        parentHash = previousBlockhash,
        timestamp = blockTime
    )
}

fun Instruction.toModel(
    accountKeys: List<String>,
    blockNumber: Long,
    blockHash: String,
    transactionHash: String,
    transactionIndex: Int,
    innerTransactionIndex: Int?
): SolanaBlockchainLog {
    val instruction = SolanaInstruction(
        programId = accountKeys[programIdIndex],
        data = data,
        accounts = accounts.map { accountKeys[it] }
    )
    val solanaLog = SolanaLog(
        blockNumber = blockNumber,
        transactionHash = transactionHash,
        blockHash = blockHash,
        instructionIndex = transactionIndex,
        innerInstructionIndex = innerTransactionIndex
    )

    return SolanaBlockchainLog(solanaLog, instruction)
}

private inline fun <reified T, reified R> ApiResponse<T>.convert(
    block: T.() -> R
) : R {
    require(result != null && error == null) {
        "Invalid response: $this"
    }

    return block(result)
}

private inline fun <reified T, reified R> ApiResponse<T>.convert(
    errorsToSkip: List<Int>,
    block: T.() -> R
) : R? {
    return if (result != null) {
        block(result)
    } else {
        val (message, code) = requireNotNull(error) { "Error field must be not null" }

        if (code in errorsToSkip) {
            null
        } else {
            error("Unknown error code: $code, message: $message")
        }
    }
}

object ErrorCodes {
    const val BLOCK_NOT_AVAILABLE = -32004
    const val SLOT_WAS_SKIPPED = -32009
}