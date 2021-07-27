package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.data.BlockMeta
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.core.common.toOptional
import com.rarible.core.logging.LoggingUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import reactor.core.publisher.Flux

class BlockScanner<OB : BlockchainBlock, OL, B : Block>(
    private val blockMapper: BlockMapper<OB, B>,
    private val blockService: BlockService<B>,
    private val blockchainClient: BlockchainClient<OB, OL>
) {
    fun scan(): Flux<BlockEvent<OB>> {
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
    private fun insertOrUpdateBlock(marker: Marker, b: OB): Flux<BlockEvent<OB>> {
        logger.info(marker, "insertOrUpdateBlock $b")
        return Flux.concat(
            blockService.getBlockHash(b.number - 1).toOptional()
                .flatMapMany { parentBlockHash ->
                    when {
                        //do nothing if parent hash not found (just started listening to blocks)
                        !parentBlockHash.isPresent -> Flux.empty()
                        //do nothing if parent hash is the same
                        parentBlockHash.get() == b.parentHash -> Flux.empty()
                        //fetch parent block and save it if parent block hash changed
                        else -> blockchainClient.getBlock(b.number - 1)
                            .flatMapMany { insertOrUpdateBlock(marker, it) }
                    }
                },
            checkNewBlock(marker, b)
        )
    }

    private fun checkNewBlock(marker: Marker, b: OB): Flux<BlockEvent<OB>> {
        return blockService.getBlockHash(b.number).toOptional()
            .flatMapMany { knownHash ->
                when {
                    !knownHash.isPresent -> {
                        logger.info(marker, "block ${b.number} ${b.hash} not found. is new block")
                        blockService.saveBlock(blockMapper.map(b))
                            .thenReturn(BlockEvent(b))
                    }
                    knownHash.isPresent && knownHash.get() != b.hash -> {
                        logger.info(marker, "block ${b.number} ${b.hash} found. hash differs")
                        blockService.saveBlock(blockMapper.map(b))
                            .thenReturn(BlockEvent(b, BlockMeta(b.number, knownHash.get(), b.parentHash, b.timestamp)))
                    }
                    else -> {
                        logger.info(marker, "block ${b.number} ${b.hash} found. hash is the same")
                        Flux.empty<BlockEvent<OB>>()
                    }
                }
            }
            .doOnSubscribe {
                logger.info(marker, "checkNewBlock ${b.number} ${b.hash}")
            }
            .doOnTerminate {
                logger.info(marker, "checkNewBlock completed ${b.number} ${b.hash}")
            }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(BlockScanner::class.java)
    }
}