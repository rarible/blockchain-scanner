package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.solana.client.dto.GetBlockRequest.TransactionDetails
import com.rarible.blockchain.scanner.solana.client.dto.SolanaBlockDto
import com.rarible.blockchain.scanner.solana.client.dto.SolanaBlockDtoParser
import com.rarible.blockchain.scanner.solana.client.dto.getSafeResult
import com.rarible.blockchain.scanner.solana.client.dto.toModel
import com.rarible.blockchain.scanner.solana.configuration.SolanaBlockchainScannerProperties
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.slf4j.LoggerFactory

class SolanaClient(
    private val api: SolanaApi,
    private val properties: SolanaBlockchainScannerProperties,
    programIds: Set<String>,
) : BlockchainClient<SolanaBlockchainBlock, SolanaBlockchainLog, SolanaDescriptor> {

    private val solanaBlockDtoParser = SolanaBlockDtoParser(
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
                val logs = block.logs.filter { log ->
                    log.instruction.programId == descriptor.programId
                }
                logger.info("Block {} has {} logs for {}, total {}. {}",
                    block.slot,
                    logs.size,
                    descriptor.programId,
                    block.logs.size,
                    block.logs.map { it.instruction.programId }.toSet()
                )
                FullBlock(
                    block,
                    logs
                )
            }
    }

    override suspend fun getFirstAvailableBlock(): SolanaBlockchainBlock {
        val slot = if (properties.scan.startFromCurrentBlock) {
            api.getLatestSlot().toModel()
        } else {
            api.getFirstAvailableBlock().toModel()
        }
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

    override suspend fun getLastBlockNumber(): Long {
        return api.getLatestSlot().toModel() // TODO not really sure that is right
    }

    companion object {

        private val logger = LoggerFactory.getLogger(SolanaClient::class.java)
        private const val LOOKUP_TABLE_META_SIZE = 56
    }
}
