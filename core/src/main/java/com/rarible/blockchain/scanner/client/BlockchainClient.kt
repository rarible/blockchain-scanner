package com.rarible.blockchain.scanner.client

import com.rarible.blockchain.scanner.model.BlockLogs
import com.rarible.blockchain.scanner.model.BlockMeta
import com.rarible.blockchain.scanner.subscriber.LogEventDescriptor
import org.slf4j.Marker
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

interface BlockchainClient<OB, OL> {

    fun getBlockEvents(block: OB, descriptor: LogEventDescriptor, marker: Marker): Mono<List<OL>>

    fun getBlockEvents(descriptor: LogEventDescriptor, range: LongRange, marker: Marker): Flux<BlockLogs<OL>>

    fun getFullBlock(hash: String): Mono<OB>

    fun getBlockMeta(id: Long): Mono<BlockMeta>


}