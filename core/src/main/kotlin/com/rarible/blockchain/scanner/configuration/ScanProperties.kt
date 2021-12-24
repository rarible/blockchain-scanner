package com.rarible.blockchain.scanner.configuration

data class ScanProperties(
    val blockConsume: BlockConsumeProperties = BlockConsumeProperties(),
    val blockPublish: BlockPublishProperties = BlockPublishProperties(),
    val logConsume: LogConsumeProperties = LogConsumeProperties()
)
