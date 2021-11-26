package com.rarible.blockchain.scanner.v2

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainBlockClient
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.service.BlockService
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach

class BlockScannerV2<BB : BlockchainBlock, B : Block>(
    private val blockMapper: BlockMapper<BB, B>,
    private val blockClient: BlockchainBlockClient<BB>,
    private val blockService: BlockService<B>
) {

    val blockEvents: Flow<BlockEvent> = flow {
        val handler = BlockHandler(
            blockMapper,
            blockClient,
            blockService,
            this
        )
        blockClient.newBlocks.onEach {
            handler.onNewBlock(it)
        }
    }

}
