package com.rarible.blockchain.scanner.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties(prefix = "blockchain.scanner.kafka")
data class KafkaProperties(
    val brokerReplicaSet: String,
    val enabled: Boolean = true,
    val numberOfPartitionsPerLogGroup: Int = 9
)
