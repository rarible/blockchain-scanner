package com.rarible.blockchain.scanner.flow.repository

import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import org.springframework.data.mongodb.repository.Query
import org.springframework.data.mongodb.repository.ReactiveMongoRepository
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux

@Repository
interface FlowLogRepository : ReactiveMongoRepository<FlowLogRecord, String> {
    @Query("{'log.type': ?0}")
    fun findByLogType(type: String): Flux<FlowLogRecord>
}
