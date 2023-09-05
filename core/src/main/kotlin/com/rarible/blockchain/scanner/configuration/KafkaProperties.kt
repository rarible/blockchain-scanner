package com.rarible.blockchain.scanner.configuration

import com.rarible.core.kafka.Compression
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties(prefix = "blockchain.scanner.kafka")
data class KafkaProperties(
    val brokerReplicaSet: String,
    val enabled: Boolean = true,
    val numberOfPartitionsPerLogGroup: Int = 9,
    val compression: Compression = Compression.SNAPPY,
)
