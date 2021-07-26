package com.rarible.blockchain.scanner.service

import com.rarible.blockchain.scanner.model.LogEvent
import org.bson.types.ObjectId
import org.slf4j.Marker
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

interface LogEventService<L : LogEvent> {

    fun delete(collection: String, log: L): Mono<L>

    fun saveOrUpdate(marker: Marker, collection: String, event: L): Mono<L>

    fun save(collection: String, log: L): Mono<L>

    fun findPendingLogs(collection: String): Flux<L>

    fun findLogEvent(collection: String, id: ObjectId): Mono<L>

    fun findAndRevert(collection: String, blockHash: String, topic: String): Flux<L>

    fun findAndDelete(collection: String, blockHash: String, topic: String, status: LogEvent.Status? = null): Flux<L>

    fun updateStatus(collection: String, log: L, status: LogEvent.Status): Mono<L>


}