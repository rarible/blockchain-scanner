package com.rarible.blockchain.scanner.reindex

import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.handler.BlockHandler
import com.rarible.blockchain.scanner.handler.LogHandler
import com.rarible.blockchain.scanner.monitoring.BlockMonitor

class BlockHandlerFactory<BB : BlockchainBlock, BL : BlockchainLog, R : LogRecord, D : Descriptor>(
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    private val blockService: BlockService,
    private val blockMonitor: BlockMonitor,
    private val properties: BlockchainScannerProperties,
) {

    fun create(logHandlers: List<LogHandler<BB, BL, R, D>>): BlockHandler<BB> {
        return BlockHandler(
            blockClient = blockchainClient,
            blockService = blockService,
            blockEventListeners = logHandlers,
            scanProperties = properties.scan,
            monitor = blockMonitor
        )
    }

}