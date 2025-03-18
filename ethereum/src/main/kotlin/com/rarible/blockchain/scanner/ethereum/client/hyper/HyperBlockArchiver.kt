package com.rarible.blockchain.scanner.ethereum.client.hyper

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rarible.blockchain.scanner.ethereum.configuration.HyperProperties
import kotlinx.coroutines.reactive.awaitFirst
import net.jpountz.lz4.LZ4Factory
import org.msgpack.jackson.dataformat.MessagePackFactory
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import java.io.ByteArrayInputStream
import java.math.BigInteger
import java.nio.ByteBuffer

/**
 * Downloads Hyper blocks from S3 storage, decompresses and deserializes them.
 */
class HyperBlockArchiver(
    private val s3Client: S3AsyncClient,
    private val hyperProperties: HyperProperties
) {
    private val logger = LoggerFactory.getLogger(HyperBlockArchiver::class.java)
    private val lz4Factory = LZ4Factory.fastestInstance()
    private val objectMapper = ObjectMapper(MessagePackFactory()).registerKotlinModule()

    /**
     * Downloads a block by its number from the S3 storage.
     * The block is stored as LZ4-compressed MessagePack data.
     *
     * @param blockNumber The block number to download
     * @return A [Mono] that emits the downloaded [HyperBlock] or an error if the block could not be downloaded or processed
     */
    suspend fun downloadBlock(blockNumber: BigInteger): HyperBlock {
        val objectKey = formatObjectKey(blockNumber.toLong())
        val bucketName = hyperProperties.s3.uri.path

        val request = GetObjectRequest.builder()
            .bucket(bucketName)
            .key(objectKey)
            .build()

        return Mono.fromFuture(s3Client.getObject(request, AsyncResponseTransformer.toBytes()))
            .map { response ->
                val compressedData = response.asByteArray()
                val decompressedData = decompressLz4(compressedData)
                val hyperBlock = deserializeMessagePack(decompressedData)

                logger.debug("Successfully downloaded and processed block $blockNumber")
                hyperBlock
            }
            .onErrorMap { e ->
                when (e) {
                    is NoSuchKeyException -> {
                        logger.warn("Block $blockNumber not found in S3 storage")
                        BlockNotFoundException("Block $blockNumber not found in S3 storage", e)
                    }
                    else -> {
                        logger.error("Failed to download or process block $blockNumber", e)
                        BlockProcessingException("Failed to download or process block $blockNumber", e)
                    }
                }
            }.awaitFirst()
    }

    /**
     * Formats the S3 object key for a specific block number.
     */
    private fun formatObjectKey(blockNumber: Long): String {
        // The format could be customized based on S3 storage structure
        // For example: "blocks/0000000/00123456.bin"
        val millions = blockNumber / 1_000_000
        return "blocks/${millions.toString().padStart(7, '0')}/${blockNumber.toString().padStart(8, '0')}.bin"
    }

    /**
     * Decompresses the LZ4-compressed data.
     */
    private fun decompressLz4(compressedData: ByteArray): ByteArray {
        val decompressor = lz4Factory.fastDecompressor()

        // Read the decompressed size from the first 4 bytes
        val decompressedSize = ByteBuffer.wrap(compressedData, 0, 4).int

        // Create the destination array
        val decompressedData = ByteArray(decompressedSize)

        // Decompress the data (skipping the first 4 bytes that contain the size)
        decompressor.decompress(
            compressedData, 4,
            decompressedData, 0, decompressedSize
        )

        return decompressedData
    }

    /**
     * Deserializes the MessagePack data into a [HyperBlock] object.
     */
    private fun deserializeMessagePack(data: ByteArray): HyperBlock {
        return objectMapper.readValue(ByteArrayInputStream(data), HyperBlock::class.java)
    }

    // Custom exceptions
    class BlockNotFoundException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
    class BlockProcessingException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
}

/**
 * Data classes representing the structure of a Hyper block.
 */
data class HyperBlock(
    val block: Block,
    val receipts: List<Receipt>,
    val systemTxs: List<Any> = emptyList()
)

data class Block(
    @JsonProperty("Reth115")
    val reth115: Reth115
)

data class Reth115(
    val header: BlockHeader,
    val body: BlockBody
)

data class BlockHeader(
    val hash: String,
    val header: Header
)

data class Header(
    val parentHash: String,
    val timestamp: String,
)

data class BlockBody(
    val transactions: List<Transaction>,
)

data class Transaction(
    val signature: List<String>,
    val transaction: TransactionData
)

data class TransactionData(
    @JsonProperty("Legacy")
    val legacy: LegacyTransaction? = null,
)

data class LegacyTransaction(
    val chainId: String,
    val nonce: String,
    val gasPrice: String,
    val gas: String,
    val to: String,
    val value: String,
    val input: String
)

data class Receipt(
    val txType: String,
    val success: Boolean,
    val cumulativeGasUsed: Long,
    val logs: List<Log>
)

data class Log(
    val address: String,
    val data: LogData
)

data class LogData(
    val topics: List<String>,
    val data: String
)
