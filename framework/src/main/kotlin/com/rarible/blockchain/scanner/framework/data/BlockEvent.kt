package com.rarible.blockchain.scanner.framework.data

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.util.scannerBlockchainEventMarks
import com.rarible.core.common.EventTimeMarks

sealed class BlockEvent<BB : BlockchainBlock> {
    abstract val number: Long
    abstract val hash: String
    abstract val mode: ScanMode
    abstract val eventTimeMarks: EventTimeMarks
    abstract val mode: ScanMode
}

sealed class NewBlockEvent<BB : BlockchainBlock> : BlockEvent<BB>() {
    abstract val block: BB
    override val number: Long
        get() = block.number
    override val hash: String
        get() = block.hash
}

data class NewStableBlockEvent<BB : BlockchainBlock>(
    override val block: BB,
    override val mode: ScanMode
) : NewBlockEvent<BB>() {

    override val eventTimeMarks = scannerBlockchainEventMarks(mode, block.getDatetime())
    override fun toString(): String = "stableBlock:$number:$hash"
}

data class NewUnstableBlockEvent<BB : BlockchainBlock>(
    override val block: BB,
    override val mode: ScanMode
) : NewBlockEvent<BB>() {

    override val eventTimeMarks = scannerBlockchainEventMarks(mode, block.getDatetime())
    override fun toString(): String = "unstableBlock:$number:$hash"
}

data class RevertedBlockEvent<BB : BlockchainBlock>(
    override val number: Long,
    override val hash: String,
    override val mode: ScanMode
) : BlockEvent<BB>() {

    override val eventTimeMarks = scannerBlockchainEventMarks(mode)
    override fun toString(): String = "revert:$number:$hash"
}
