package com.rarible.blockchain.scanner.framework.client

import com.rarible.blockchain.scanner.data.BlockLogs
import com.rarible.blockchain.scanner.data.TransactionMeta
import com.rarible.blockchain.scanner.subscriber.LogEventDescriptor
import org.slf4j.Marker
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.*

interface BlockchainClient<OB : BlockchainBlock, OL> {

    fun listenNewBlocks(): Flux<OB>

    fun getBlock(id: Long): Mono<OB>

    fun getBlock(hash: String): Mono<OB>

    fun getLastBlockNumber(): Mono<Long>

    fun getBlockEvents(block: OB, descriptor: LogEventDescriptor, marker: Marker): Mono<List<OL>>

    fun getBlockEvents(descriptor: LogEventDescriptor, range: LongRange, marker: Marker): Flux<BlockLogs<OL>>

    fun getTransactionMeta(transactionHash: String): Mono<Optional<TransactionMeta>>


}