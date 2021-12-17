package com.rarible.blockchain.scanner.configuration

data class ScanProperties(
    val blockPublish: BlockPublishProperties = BlockPublishProperties(),
    val eventConsume: EventConsumeProperties = EventConsumeProperties()
)
