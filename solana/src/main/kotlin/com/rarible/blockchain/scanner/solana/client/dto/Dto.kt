package com.rarible.blockchain.scanner.solana.client.dto

import com.fasterxml.jackson.databind.JsonNode
import com.rarible.blockchain.scanner.framework.data.TransactionMeta
import com.rarible.blockchain.scanner.solana.client.SolanaBlockEvent
import com.rarible.blockchain.scanner.solana.client.SolanaBlockEvent.*
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainBlock
import com.rarible.blockchain.scanner.solana.client.dto.SolanaTransactionDto.Instruction
import com.rarible.blockchain.scanner.solana.client.metaplex.CreateMetadataAccountArgs
import com.rarible.blockchain.scanner.solana.client.metaplex.parseMetadataInstruction
import org.bitcoinj.core.Base58

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
            "encoding" to "jsonParsed",
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
    val result: T
)

data class SolanaTransactionMetaDto(
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

data class SolanaTransactionDto(
    val transaction: Details,
    val meta: Meta,
) {
    data class Instruction(
        val program: String?,
        val data: String?,
        val programId: String,
        val parsed: JsonNode?
    )

    data class Message(
        val recentBlockhash: String?,
        val instructions: List<Instruction>
    )

    data class Details(
        val message: Message,
        val signatures: List<String>
    )

    data class Meta(
        val innerInstructions: List<Message>
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

fun SolanaBlockDto.toModel(slot: Long) = SolanaBlockchainBlock(
    slot = slot,
    parentSlot = parentSlot,
    number = blockHeight,
    hash = blockhash,
    parentHash = previousBlockhash,
    timestamp = blockTime
)

fun SolanaTransactionMetaDto.toModel() = TransactionMeta(
    hash = transaction.signatures.first(),
    blockHash = transaction.message.recentBlockhash
)

fun SolanaTransactionDto.toModel(): List<SolanaBlockEvent> {
    val instructions = transaction.message.instructions + meta.innerInstructions.flatMap { it.instructions }

    return instructions.mapNotNull { it.toModel() }
}

fun Instruction.toModel(): SolanaBlockEvent? {
    return when (program) {
        "spl-token" -> parseSplToken()
        else -> {
            when (programId) {
                "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s" -> parseTokenMetadata()
                else -> null
            }
        }
    }
}

private fun Instruction.parseTokenMetadata(): SolanaBlockEvent? {
    requireNotNull(data) { "Program data is missed: $this" }

    return when (val metadataInstruction = Base58.decode(data).parseMetadataInstruction()) {
        is CreateMetadataAccountArgs -> return SolanaCreateTokenMetadataEvent(
            name = metadataInstruction.data.name,
            uri = metadataInstruction.data.uri,
            symbol = metadataInstruction.data.symbol,
            creators = metadataInstruction.data.creators?.map { Base58.encode(it.address) }
                ?: emptyList()
        )
        else -> null
    }
}

private fun Instruction.parseSplToken(): SolanaBlockEvent? {
    val params = requireNotNull(parsed) { "Parsed details of transaction are missed: $this" }
    val type = params["type"].textValue()

    if (type == "mintTo") {
        val account = params["info"]["account"].textValue()
        val amount = params["info"]["amount"].textValue()
        val mint = params["info"]["mint"].textValue()

        return SolanaMintEvent(account, amount.toBigInteger(), mint)
    }

    return null
}