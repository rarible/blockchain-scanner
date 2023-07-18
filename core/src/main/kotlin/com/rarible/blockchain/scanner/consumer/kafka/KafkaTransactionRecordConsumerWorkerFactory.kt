package com.rarible.blockchain.scanner.consumer.kafka

import com.rarible.blockchain.scanner.configuration.KafkaProperties
import com.rarible.blockchain.scanner.consumer.TransactionRecordConsumerWorkerFactory
import com.rarible.blockchain.scanner.consumer.TransactionRecordFilter
import com.rarible.blockchain.scanner.consumer.TransactionRecordMapper
import com.rarible.blockchain.scanner.framework.listener.TransactionRecordEventListener
import com.rarible.blockchain.scanner.util.getLogTopicPrefix
import com.rarible.core.daemon.DaemonWorkerProperties
import com.rarible.core.kafka.RaribleKafkaBatchEventHandler
import com.rarible.core.kafka.RaribleKafkaConsumerFactory
import com.rarible.core.kafka.RaribleKafkaConsumerSettings
import com.rarible.core.kafka.RaribleKafkaConsumerWorker

class KafkaTransactionRecordConsumerWorkerFactory(
    host: String,
    environment: String,
    blockchain: String,
    service: String,
    private val properties: KafkaProperties,
    private val daemonProperties: DaemonWorkerProperties,
) : TransactionRecordConsumerWorkerFactory {

    private val topicPrefix = getLogTopicPrefix(environment, service, blockchain, "transaction")
    private val consumerWorkerFactory = RaribleKafkaConsumerFactory(
        env = environment,
        host = host,
    )

    override fun <T> create(
        listener: TransactionRecordEventListener,
        transactionRecordType: Class<T>,
        transactionRecordMapper: TransactionRecordMapper<T>,
        logRecordFilters: List<TransactionRecordFilter<T>>,
        workerCount: Int,
    ): RaribleKafkaConsumerWorker<T> {
        val handler = KafkaTransactionRecordEventHandler(listener, transactionRecordMapper, logRecordFilters)
        return consumerWorkerFactory.createWorker(
            settings = RaribleKafkaConsumerSettings(
                batchSize = daemonProperties.consumerBatchSize,
                concurrency = workerCount,
                group = listener.id,
                async = false,
                hosts = properties.brokerReplicaSet,
                topic = "$topicPrefix.${listener.groupId}",
                valueClass = transactionRecordType,
            ),
            handler = object : RaribleKafkaBatchEventHandler<T> {
                override suspend fun handle(events: List<T>) {
                    handler.handle(events)
                }
            },
        )
    }
}