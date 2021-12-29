package com.rarible.blockchain.scanner.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties(prefix = "blockchain.scanner.kafka")
data class KafkaProperties(
    val brokerReplicaSet: String,
    val maxPollRecords: Int = 10
)
