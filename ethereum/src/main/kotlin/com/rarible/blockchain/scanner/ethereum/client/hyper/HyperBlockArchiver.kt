package com.rarible.blockchain.scanner.ethereum.client.hyper

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.BinaryNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rarible.blockchain.scanner.ethereum.configuration.HyperArchiveProperties
import io.daonomic.rpc.domain.Binary
import kotlinx.coroutines.reactive.awaitFirst
import net.jpountz.lz4.LZ4FrameInputStream
import org.msgpack.jackson.dataformat.MessagePackFactory
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.RequestPayer
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.math.BigInteger

class HyperBlockArchiver(
    private val s3Client: S3AsyncClient,
    private val hyperProperties: HyperArchiveProperties
) {
    private val logger = LoggerFactory.getLogger(HyperBlockArchiver::class.java)

    private val objectMapper = ObjectMapper(MessagePackFactory()).apply {
        registerKotlinModule()
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    }

    suspend fun downloadBlock(blockNumber: BigInteger): HyperBlock {
        val objectKey = formatObjectKey(blockNumber.toLong())
        val bucketName = hyperProperties.s3.uri.host

        val request = GetObjectRequest.builder()
            .bucket(bucketName)
            .key(objectKey)
            .requestPayer(RequestPayer.REQUESTER)
            .build()

        return Mono.fromFuture(s3Client.getObject(request, AsyncResponseTransformer.toBytes()))
            .map { response ->
                val compressedData = response.asByteArray()
                val decompressedData = decompressLz4(compressedData)
                val hyperBlock = deserializeMessagePack(blockNumber, decompressedData)
                hyperBlock
            }
            .onErrorMap { e ->
                when (e) {
                    is NoSuchKeyException -> {
                        logger.warn("Block $blockNumber not found in S3 storage")
                        BlockNotFoundException("Block $blockNumber not found in S3 storage", e)
                    }
                    else -> {
                        logger.warn("Failed to download or process block $blockNumber", e)
                        BlockProcessingException("Failed to download or process block $blockNumber", e)
                    }
                }
            }.awaitFirst()
    }

    private fun formatObjectKey(blockNumber: Long): String {
        val thousands = blockNumber / 1_000
        val topLevelDir = blockNumber / 1_000_000
        val secondLevelDir = thousands * 1000
        return "$topLevelDir/$secondLevelDir/$blockNumber.rmp.lz4"
    }

    private fun decompressLz4(compressedData: ByteArray): ByteArray {
        ByteArrayInputStream(compressedData).use { input ->
            LZ4FrameInputStream(input).use { lz4Input ->
                ByteArrayOutputStream().use { output ->
                    val buffer = ByteArray(8192)
                    var bytesRead: Int
                    while (lz4Input.read(buffer).also { bytesRead = it } != -1) {
                        output.write(buffer, 0, bytesRead)
                    }
                    return output.toByteArray()
                }
            }
        }
    }

    private fun deserializeMessagePack(block: BigInteger, data: ByteArray): HyperBlock {
        if (hyperProperties.logBlockJson) {
            val tree = objectMapper.readTree(data)
            val convertedTree = convertBinaryNodes(tree)
            val prettyJson = jacksonObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(convertedTree)
            logger.info("Block: number=$block, json=\n: $prettyJson")
        }
        return objectMapper.readValue(data, Array<HyperBlock>::class.java).single()
    }

    // Custom exceptions
    class BlockNotFoundException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
    class BlockProcessingException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

    private fun convertBinaryNodes(node: JsonNode): JsonNode {
        return when (node) {
            is ObjectNode -> {
                val fields = node.fields()
                val newObj = JsonNodeFactory.instance.objectNode()
                fields.forEachRemaining { entry ->
                    newObj.set<JsonNode>(entry.key, convertBinaryNodes(entry.value))
                }
                newObj
            }
            is ArrayNode -> {
                val newArray = JsonNodeFactory.instance.arrayNode()
                node.forEach { child ->
                    newArray.add(convertBinaryNodes(child))
                }
                newArray
            }
            is BinaryNode -> {
                val binaryValue = node.binaryValue()
                val value = when {
                    binaryValue.size >= 20 -> Binary.apply(binaryValue).prefixed()
                    binaryValue.isEmpty() -> Binary.empty().prefixed()
                    else -> BigInteger(binaryValue).toString()
                }
                TextNode(value)
            }
            else -> node
        }
    }
}

data class HyperBlock(
    val block: Block,
    val receipts: List<Receipt>,
)

data class Block(
    @JsonProperty("Reth115")
    val reth115: Reth115
)

data class Reth115(
    val header: BlockHeader,
    val body: BlockBody
)

@Suppress("ArrayInDataClass")
data class BlockHeader(
    val hash: ByteArray,
    val header: Header
)

@Suppress("ArrayInDataClass")
data class Header(
    val number: ByteArray,
    val parentHash: ByteArray,
    val timestamp: ByteArray,
    val sha3Uncles: ByteArray,
    val nonce: ByteArray,
    val logsBloom: ByteArray,
    val transactionsRoot: ByteArray,
    val stateRoot: ByteArray,
    val miner: ByteArray,
    val difficulty: ByteArray,
    val extraData: ByteArray,
    val gasLimit: ByteArray,
    val gasUsed: ByteArray,
)

data class BlockBody(
    val transactions: List<Transaction>,
)

data class Transaction(
    val signature: List<ByteArray>,
    val transaction: TransactionData
)

data class TransactionData(
    @JsonProperty("Legacy")
    val legacy: LegacyTransaction? = null,
    @JsonProperty("Eip1559")
    val eip1559: Eip1559Transaction? = null,
) {
    fun getCommonTransaction(): CommonTransaction {
        return legacy ?: eip1559 ?: throw IllegalArgumentException("Transaction type not supported")
    }
}

interface CommonTransaction {
    val chainId: ByteArray
    val nonce: ByteArray
    val to: ByteArray?
    val value: ByteArray
    val input: ByteArray
    val gas: ByteArray
    val gasPrice: ByteArray
}

@Suppress("ArrayInDataClass")
data class LegacyTransaction(
    override val chainId: ByteArray,
    override val nonce: ByteArray,
    override val to: ByteArray?,
    override val value: ByteArray,
    override val input: ByteArray,
    override val gas: ByteArray,
    override val gasPrice: ByteArray,
) : CommonTransaction

@Suppress("ArrayInDataClass")
data class Eip1559Transaction(
    override val chainId: ByteArray,
    override val nonce: ByteArray,
    override val to: ByteArray?,
    override val value: ByteArray,
    override val input: ByteArray,
    override val gas: ByteArray,
    val maxFeePerGas: ByteArray,
    val maxPriorityFeePerGas: String,
) : CommonTransaction {
    override val gasPrice = maxFeePerGas
}

data class Receipt(
    @JsonProperty("tx_type")
    val txType: String,
    val success: Boolean,
    val cumulativeGasUsed: Long,
    val logs: List<Log>
)

@Suppress("ArrayInDataClass")
data class Log(
    val address: ByteArray,
    val data: LogData
)

@Suppress("ArrayInDataClass")
data class LogData(
    val topics: List<ByteArray>,
    val data: ByteArray
)
