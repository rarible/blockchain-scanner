package com.rarible.blockchain.scanner.framework.mapper

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log

/**
 * Mapper (i.e. converter) of original Blockchain Log to persistent Log, which will be stored in storage of specific
 * BlockchainScanner implementation as part of LogRecord. At the end implementation, it could be stored as nested object
 * or in some other way.
 */
interface LogMapper<BB : BlockchainBlock, BL : BlockchainLog, L : Log> {

    /**
     * Convert original Blockchain Log to persistent Log data
     *
     * @param block original Blockchain Block
     * @param log original Blockchain Log
     * @param index index of Log in Block
     * @param minorIndex index of Log in context of LogEvent
     * (if there are several LogRecords for single Log, they will be ordered by this index)
     * @param descriptor descriptor of subscriber, who provide LogRecords
     */
    fun map(
        block: BB,
        log: BL,
        index: Int,
        minorIndex: Int,
        descriptor: Descriptor
    ): L
}