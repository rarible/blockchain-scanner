package com.rarible.blockchain.scanner.solana.client.dto

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonSetter
import com.fasterxml.jackson.annotation.Nulls
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainBlock
import com.rarible.blockchain.scanner.solana.client.SolanaBlockchainLog
import com.rarible.blockchain.scanner.solana.client.SolanaInstruction
import com.rarible.blockchain.scanner.solana.client.dto.SolanaTransactionDto.Instruction
import com.rarible.blockchain.scanner.solana.model.SolanaLog
import java.math.BigInteger

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
            "maxSupportedTransactionVersion" to 0,
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
        mapOf(
            "maxSupportedTransactionVersion" to 0,
            "commitment" to "confirmed"
        )
    )
)

class GetAccountInfo(
    address: String,
    encoding: Encoding,
) : Request(
    method = "getAccountInfo",
    params = listOf(
        address,
        mapOf(
            "encoding" to encoding.value
        )
    )
)

class GetBalance(
    address: String,
) : Request(
    method = "getBalance",
    params = listOf(address)
)

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
data class ApiResponse<T>(
    val result: T?,
    val error: Error?,
    val jsonrpc: String?,
    val id: Long
) {
    data class Error(
        val message: String,
        val code: Int
    )
}

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
data class SolanaAccountInfoDto(
    val value: Value,
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Value(
        val data: Data,
        val owner: String?,
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Data(
        val parsed: Parsed,
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Parsed(
        val info: Info,
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Info(
        val extensions: List<Extension>
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Extension(
        val extension: String,
        val state: Map<String, Any?>?,
    ) {

        fun toMetadata(): Metadata {
            return objectMapper.convertValue(state, Metadata::class.java)
        }

        companion object {
            private val objectMapper = ObjectMapper().registerModules().registerKotlinModule()
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Metadata(
        val mint: String?,
        val name: String?,
        val symbol: String?,
        val updateAuthority: String?,
        val uri: String?,
        val additionalMetadata: List<List<String>>,
    )
}

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
data class SolanaAccountBase64InfoDto(
    val value: Value
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Value(
        val data: List<String>,
    )
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class SolanaTransactionDto(
    val transaction: Details?,
    val meta: Meta?
) {
    @get:JsonIgnore
    val isSuccessful: Boolean
        get() =
            meta != null && meta.err == null

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Instruction(
        val accounts: List<Int>,
        val data: String,
        val programIdIndex: Int
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class InnerInstruction(
        val index: Int,
        val instructions: List<Instruction?>,
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Message(
        val recentBlockhash: String?,
        val accountKeys: List<String>,
        val instructions: List<Instruction?>
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Details(
        val message: Message,
        val signatures: List<String>
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Meta(
        @JsonSetter(nulls = Nulls.AS_EMPTY)
        val innerInstructions: List<InnerInstruction> = emptyList(),
        val loadedAddresses: LoadedAddresses?,
        val err: Any?
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class LoadedAddresses(
        val writable: List<String>,
        val readonly: List<String>
    )
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class SolanaBlockDto(
    val parentSlot: Long,
    val blockhash: String,
    val previousBlockhash: String?,
    val blockHeight: Long,
    val blockTime: Long,
    @JsonSetter(nulls = Nulls.AS_EMPTY)
    val transactions: List<SolanaTransactionDto> = emptyList()
) {
    companion object {
        val errorsToSkip = listOf(
            -32004, // BLOCK_NOT_AVAILABLE,
            -32009, // SLOT_WAS_SKIPPED_OR_MISSING_IN_LONG_TERM_STORAGE,
            -32007, // SLOT_WAS_SKIPPED_OR_MISSING_DUE_TO_LEDGER_JUMP_TO_RECENT_SNAPSHOT
        )
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class SolanaBalanceDto(val value: BigInteger)

enum class Encoding(val value: String) {
    JSON_PARSED("jsonParsed"),
    BASE64("base64")
}

class SolanaBlockDtoParser(
    private val programIds: Set<String>
) {
    fun toModel(blockDto: SolanaBlockDto, slot: Long): SolanaBlockchainBlock = with(blockDto) {
        val logs = transactions.flatMapIndexed { transactionIndex, transactionDto ->
            if (!transactionDto.isSuccessful) {
                return@flatMapIndexed emptyList()
            }
            val transaction = transactionDto.transaction ?: return@flatMapIndexed emptyList()
            val accountKeys = transaction.message.accountKeys +
                (transactionDto.meta?.loadedAddresses?.let { it.writable + it.readonly } ?: emptyList())
            val transactionHash = transactionDto.transaction.signatures.first()
            val result = arrayListOf<SolanaBlockchainLog>().apply {
                this += transaction.message.instructions.mapIndexedNotNull { instructionIndex, instruction ->
                    instruction?.toModel(
                        accountKeys = accountKeys,
                        blockNumber = slot,
                        blockHash = blockhash,
                        transactionHash = transactionHash,
                        transactionIndex = transactionIndex,
                        instructionIndex = instructionIndex,
                        innerInstructionIndex = null
                    )
                }

                transactionDto.meta?.let { meta ->
                    this += meta.innerInstructions.flatMap { innerInstruction ->
                        innerInstruction.instructions.mapIndexedNotNull { innerInstructionIndex, instruction ->
                            instruction?.toModel(
                                accountKeys = accountKeys,
                                blockNumber = slot,
                                blockHash = blockhash,
                                transactionHash = transactionHash,
                                transactionIndex = transactionIndex,
                                instructionIndex = innerInstruction.index,
                                innerInstructionIndex = innerInstructionIndex
                            )
                        }
                    }
                }
            }

            result
        }

        return SolanaBlockchainBlock(
            slot = slot,
            parentSlot = parentSlot,
            logs = logs,
            hash = blockhash,
            parentHash = previousBlockhash,
            timestamp = blockTime
        )
    }

    private fun Instruction.toModel(
        accountKeys: List<String>,
        blockNumber: Long,
        blockHash: String,
        transactionHash: String,
        transactionIndex: Int,
        instructionIndex: Int,
        innerInstructionIndex: Int?
    ): SolanaBlockchainLog? {
        val programId = accountKeys[programIdIndex]
        if (programIds.isNotEmpty() && programId !in programIds) {
            return null
        }
        val instruction = SolanaInstruction(
            programId = programId,
            data = data,
            accounts = accounts.map { accountKeys[it] }
        )
        val solanaLog = SolanaLog(
            blockNumber = blockNumber,
            transactionHash = transactionHash,
            blockHash = blockHash,
            transactionIndex = transactionIndex,
            instructionIndex = instructionIndex,
            innerInstructionIndex = innerInstructionIndex
        )

        return SolanaBlockchainLog(solanaLog, instruction)
    }
}

fun ApiResponse<Long>.toModel(): Long = convert { this }

private inline fun <reified T, reified R> ApiResponse<T>.convert(
    block: T.() -> R
): R {
    require(result != null && error == null) {
        "Invalid response: $this"
    }

    return block(result)
}

fun <T> ApiResponse<T>.getSafeResult(
    slot: Long,
    errorsToSkip: List<Int>
): T? {
    return if (result != null) {
        result
    } else {
        val (message, code) = requireNotNull(error) { "Invalid ApiResponse for block $slot, both 'result' and 'error' fields are null" }

        if (code in errorsToSkip) {
            null
        } else {
            error("Unknown error code: $code, message: $message")
        }
    }
}
