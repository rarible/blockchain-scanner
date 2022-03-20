package com.rarible.blockchain.scanner.solana.migration

import com.github.cloudyrock.mongock.ChangeLog
import com.github.cloudyrock.mongock.ChangeSet
import com.github.cloudyrock.mongock.driver.mongodb.springdata.v3.decorator.impl.MongockTemplate
import io.changock.migration.api.annotations.NonLockGuarded
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.index.Index

@ChangeLog(order = "00001")
class ChangeLog00001 {

    private val logger = LoggerFactory.getLogger(ChangeLog00001::class.java)

    @ChangeSet(id = "createInitialIndexes", order = "00001", author = "Sergey.Patrikeev", runAlways = true)
    fun createInitialIndexes(
        template: MongockTemplate,
        @NonLockGuarded solanaLogEventSubscriberHolder: SolanaLogEventSubscriberHolder
    ) {
        val recordsCollections =
            solanaLogEventSubscriberHolder.subscribers.map { it.getDescriptor().collection }.toSet()
        recordsCollections.forEach { createInitialIndices(template, it) }
    }

    private fun createInitialIndices(template: MongockTemplate, collection: String) {
        val indexOps = template.indexOps(collection)
        val indexes = listOf(
            Index()
                .on("log.blockHash", Sort.Direction.ASC)
                .on("_id", Sort.Direction.ASC)
                .background()
        )
        indexes.forEach {
            logger.info("Creating index ${it.indexKeys} on $collection")
            indexOps.ensureIndex(it)
        }
    }
}
