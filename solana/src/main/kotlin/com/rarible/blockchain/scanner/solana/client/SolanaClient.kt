package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.solana.client.dto.GetBlockRequest.TransactionDetails
import com.rarible.blockchain.scanner.solana.client.dto.SolanaBlockDto
import com.rarible.blockchain.scanner.solana.client.dto.SolanaBlockDtoParser
import com.rarible.blockchain.scanner.solana.client.dto.convert
import com.rarible.blockchain.scanner.solana.client.dto.getSafeResult
import com.rarible.blockchain.scanner.solana.client.dto.toModel
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.Base64

class SolanaClient(
    private val api: SolanaApi,
    programIds: Set<String>
) : BlockchainClient<SolanaBlockchainBlock, SolanaBlockchainLog, SolanaDescriptor> {
    private val solanaBlockDtoParser = SolanaBlockDtoParser(
        client = this,
        programIds = programIds
    )

    override val newBlocks: Flow<SolanaBlockchainBlock>
        get() = flow {
            var latestSlot: Long = -1

            while (true) {
                val slot = getLatestSlot()

                if (slot != latestSlot) {
                    val block = getBlock(slot)
                    latestSlot = slot
                    block?.let { emit(it) }
                }
            }
        }

    suspend fun getAccountInfo(account: String): List<String> {
        val accountInfo = api.getAccountInfo(account).convert { this.value.data }

        require(accountInfo.size == 2)
        require(accountInfo[1] == "base64")

        val base64 = Base64.getDecoder().decode(accountInfo[0])
        val buffer = ByteBuffer.wrap(base64).apply { order(ByteOrder.LITTLE_ENDIAN) }

        repeat(LOOKUP_TABLE_META_SIZE) { buffer.get() }

        val result = mutableListOf<String>()

        while (buffer.hasRemaining())  {
            val byteArray = ByteArray(32)

            buffer.get(byteArray)

            result += Base58.encode(byteArray)
        }

        return result
    }

    suspend fun getLatestSlot(): Long = api.getLatestSlot().toModel()

    override suspend fun getBlocks(numbers: List<Long>): List<SolanaBlockchainBlock> {
        val blocks = api.getBlocks(numbers, TransactionDetails.Full)
            .mapValues { it.value.getSafeResult(it.key, SolanaBlockDto.errorsToSkip) }

        return blocks.mapNotNull { (slot, block) -> block?.let { solanaBlockDtoParser.toModel(block, slot) } }
    }

    override suspend fun getBlock(number: Long): SolanaBlockchainBlock? {
        val blockDto = api.getBlock(number, TransactionDetails.Full).getSafeResult(number, SolanaBlockDto.errorsToSkip)

        return blockDto?.let { solanaBlockDtoParser.toModel(it, number) }
    }

    override fun getBlockLogs(
        descriptor: SolanaDescriptor,
        blocks: List<SolanaBlockchainBlock>,
        stable: Boolean
    ): Flow<FullBlock<SolanaBlockchainBlock, SolanaBlockchainLog>> {
        return blocks.asFlow()
            .map { block ->
                FullBlock(
                    block,
                    block.logs.filter { log -> log.instruction.programId == descriptor.programId }
                )
            }
    }

    override suspend fun getFirstAvailableBlock(): SolanaBlockchainBlock {
        val slot = api.getFirstAvailableBlock().toModel()
        val root = getBlock(slot)

        return if (root == null) {
            error("Can't find root block")
        } else {
            if (root.hash != root.parentHash) {
                logger.error("Root's parent hash != hash")
            }

            root
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SolanaClient::class.java)
        private const val LOOKUP_TABLE_META_SIZE = 56
    }
}
