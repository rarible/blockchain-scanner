package com.rarible.blockchain.scanner.ethereum.reduce

import com.rarible.blockchain.scanner.ethereum.model.EthereumEntityEvent
import com.rarible.core.entity.reducer.model.Entity
import com.rarible.core.entity.reducer.service.Reducer
import org.slf4j.LoggerFactory

class LoggingReducer<Id, Event : EthereumEntityEvent<Event>, E : Entity<Id, Event, E>> : Reducer<Event, E> {
    override suspend fun reduce(entity: E, event: Event): E {
        val log = event.log

        logger.info(
            "event: {}, status: {}, block: {}, logIndex: {}, minorLogIndex: {}, index: {}, id: {}",
            event::class.java.simpleName,
            log.status,
            log.blockNumber,
            log.logIndex,
            log.minorLogIndex,
            log.index,
            entity.id
        )
        return entity
    }

    companion object {
        private val logger = LoggerFactory.getLogger(LoggingReducer::class.java)
    }
}
