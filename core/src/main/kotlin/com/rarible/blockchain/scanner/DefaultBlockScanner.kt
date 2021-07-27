package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.data.BlockMeta
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.core.common.toOptional
import com.rarible.core.logging.LoggingUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import reactor.core.publisher.Flux

class DefaultBlockScanner<OB : BlockchainBlock, OL : BlockchainLog, B : Block>(
    private val blockchainClient: BlockchainClient<OB, OL>,
    private val blockMapper: BlockMapper<OB, B>,
    private val blockService: BlockService<B>
) : BlockScanner {

    override fun scan(): Flux<BlockEvent> {
        return LoggingUtils.withMarkerFlux { marker ->
            blockchainClient.listenNewBlocks()
                .concatMap { getNewBlocks(marker, it).concatMap { newBlock -> insertOrUpdateBlock(marker, newBlock) } }
        }
    }

    private fun getNewBlocks(marker: Marker, newBlock: OB): Flux<OB> {
        return blockService.getLastBlock().toOptional()
            .flatMapMany { lastKnown ->
                when {
                    !lastKnown.isPresent -> Flux.just(newBlock)
                    else -> {
                        val range = (lastKnown.get() + 1) until newBlock.number
                        if (range.last >= range.first) {
                            logger.info(marker, "getting missing blocks: $range")
                        }
                        Flux.concat(
                            Flux.fromIterable(range.asIterable())
                                .concatMap { blockchainClient.getBlock(it) },
                            Flux.just(newBlock)
                        )
                    }
                }
            }
    }

    /**
     * when inserting/updating block we need to inspect parent blocks if chain was reorganized
     */
    private fun insertOrUpdateBlock(marker: Marker, newBlock: OB): Flux<BlockEvent> {
        logger.info(marker, "insertOrUpdateBlock $newBlock")
        return Flux.concat(
            blockService.getBlockHash(newBlock.number - 1).toOptional()
                .flatMapMany { parentBlockHash ->
                    when {
                        //do nothing if parent hash not found (just started listening to blocks)
                        !parentBlockHash.isPresent -> Flux.empty()
                        //do nothing if parent hash is the same
                        parentBlockHash.get() == newBlock.parentHash -> Flux.empty()
                        //fetch parent block and save it if parent block hash changed
                        else -> blockchainClient.getBlock(newBlock.number - 1)
                            .flatMapMany { insertOrUpdateBlock(marker, it) }
                    }
                },
            checkNewBlock(marker, newBlock)
        )
    }

    private fun checkNewBlock(marker: Marker, block: OB): Flux<BlockEvent> {
        return blockService.getBlockHash(block.number).toOptional()
            .flatMapMany { knownHash ->
                when {
                    !knownHash.isPresent -> {
                        logger.info(marker, "block ${block.number} ${block.hash} not found. is new block")
                        blockService.saveBlock(blockMapper.map(block))
                            .thenReturn(BlockEvent(block))
                    }
                    knownHash.isPresent && knownHash.get() != block.hash -> {
                        logger.info(marker, "block ${block.number} ${block.hash} found. hash differs")
                        blockService.saveBlock(blockMapper.map(block))
                            .thenReturn(
                                BlockEvent(
                                    block.meta,
                                    BlockMeta(block.number, knownHash.get(), block.parentHash, block.timestamp)
                                )
                            )
                    }
                    else -> {
                        logger.info(marker, "block ${block.number} ${block.hash} found. hash is the same")
                        Flux.empty<BlockEvent>()
                    }
                }
            }
            .doOnSubscribe {
                logger.info(marker, "checkNewBlock ${block.number} ${block.hash}")
            }
            .doOnTerminate {
                logger.info(marker, "checkNewBlock completed ${block.number} ${block.hash}")
            }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DefaultBlockScanner::class.java)
    }
}