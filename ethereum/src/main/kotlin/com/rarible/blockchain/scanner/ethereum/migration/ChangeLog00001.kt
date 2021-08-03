package com.rarible.blockchain.scanner.ethereum.migration

import com.github.cloudyrock.mongock.ChangeLog
import com.github.cloudyrock.mongock.ChangeSet
import com.github.cloudyrock.mongock.driver.mongodb.springdata.v3.decorator.impl.MongockTemplate
import io.changock.migration.api.annotations.NonLockGuarded
import org.bson.Document
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.index.Index
import org.springframework.data.mongodb.core.index.PartialIndexFilter
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update

@ChangeLog(order = "00001")
class ChangeLog00001 {

    @ChangeSet(id = "fillMinorLogIndex", order = "0000", author = "eugene")
    fun fillMinorLogIndex(
        template: MongockTemplate,
        @NonLockGuarded holderEthereum: EthereumLogEventSubscriberHolder
    ) {
        val collections = holderEthereum.subscribers.map { it.getDescriptor().collection }.toSet()
        collections.forEach {
            template.updateMulti(
                Query(Criteria.where("log.minorLogIndex").exists(false)),
                Update().set("log.minorLogIndex", 0),
                it
            )
        }
    }

    @ChangeSet(id = "ensureInitialIndexes", order = "00001", author = "eugene")
    fun ensureInitialIndexes(
        template: MongockTemplate,
        @NonLockGuarded holderEthereum: EthereumLogEventSubscriberHolder
    ) {
        val collections = holderEthereum.subscribers.map { it.getDescriptor().collection }.toSet()
        collections.forEach { createInitialIndices(template, it) }
    }

    private fun createInitialIndices(template: MongockTemplate, collection: String) {
        val indexOps = template.indexOps(collection)
        indexOps.ensureIndex(
            Index()
                .on("log.transactionHash", Sort.Direction.ASC)
                .on("log.topic", Sort.Direction.ASC)
                .on("log.index", Sort.Direction.ASC)
                .on("log.minorLogIndex", Sort.Direction.ASC)
                .on("log.visible", Sort.Direction.ASC)
                .on("log.address", Sort.Direction.ASC)
                .named(VISIBLE_INDEX_NAME)
                .background()
                .unique()
                .partial(PartialIndexFilter.of(Document("visible", true)))
        )

        indexOps.ensureIndex(
            Index()
                .on("log.transactionHash", Sort.Direction.ASC)
                .on("log.blockHash", Sort.Direction.ASC)
                .on("log.logIndex", Sort.Direction.ASC)
                .on("log.minorLogIndex", Sort.Direction.ASC)
                .named("log_transactionHash_1_log_blockHash_1_log_logIndex_1_log_minorLogIndex_1")
                .background()
                .unique()
        )

        indexOps.ensureIndex(
            Index()
                .on("log.status", Sort.Direction.ASC)
                .named("log_status")
                .background()
        )

        indexOps.ensureIndex(
            Index()
                .on("log.blockHash", Sort.Direction.ASC)
                .named("log_blockHash")
                .background()
        )
    }

    companion object {
        const val VISIBLE_INDEX_NAME =
            "log_transactionHash_1_log_topic_1_log_index_1_log_minorLogIndex_1_log_visible_1_log_address_1"
    }
}