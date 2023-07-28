package com.rarible.blockchain.scanner.consumer.kafka

import com.rarible.blockchain.scanner.configuration.KafkaProperties
import com.rarible.blockchain.scanner.consumer.LogRecordConsumerWorkerFactory
import com.rarible.blockchain.scanner.consumer.LogRecordFilter
import com.rarible.blockchain.scanner.consumer.LogRecordMapper
import com.rarible.blockchain.scanner.framework.listener.LogRecordEventListener
import com.rarible.blockchain.scanner.util.getLogTopicPrefix
import com.rarible.core.daemon.DaemonWorkerProperties
import com.rarible.core.kafka.RaribleKafkaBatchEventHandler
import com.rarible.core.kafka.RaribleKafkaConsumerFactory
import com.rarible.core.kafka.RaribleKafkaConsumerSettings
import com.rarible.core.kafka.RaribleKafkaConsumerWorker

class KafkaLogRecordConsumerWorkerFactory(
    host: String,
    environment: String,
    blockchain: String,
    service: String,
    private val properties: KafkaProperties,
    private val daemonProperties: DaemonWorkerProperties,
) : LogRecordConsumerWorkerFactory {

    private val topicPrefix = getLogTopicPrefix(environment, service, blockchain, "log")
    private val consumerWorkerFactory = RaribleKafkaConsumerFactory(
        env = environment,
        host = host,
    )

    override fun <T> create(
        listener: LogRecordEventListener,
        logRecordType: Class<T>,
        logRecordMapper: LogRecordMapper<T>,
        logRecordFilters: List<LogRecordFilter<T>>,
        workerCount: Int,
    ): RaribleKafkaConsumerWorker<T> {
        val handler = KafkaLogRecordEventHandler(listener, logRecordMapper, logRecordFilters)
        return consumerWorkerFactory.createWorker(
            settings = RaribleKafkaConsumerSettings(
                batchSize = daemonProperties.consumerBatchSize,
                concurrency = workerCount,
                group = listener.id,
                async = false,
                hosts = properties.brokerReplicaSet,
                topic = "$topicPrefix.${listener.groupId}",
                valueClass = logRecordType,
            ),
            handler = object : RaribleKafkaBatchEventHandler<T> {
                override suspend fun handle(events: List<T>) {
                    handler.handle(events)
                }
            },
        )
    }
}
