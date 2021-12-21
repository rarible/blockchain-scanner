package com.rarible.blockchain.scanner.configuration

data class ScanProperties(
    val blockBatchLoad: BlockBatchLoadProperties = BlockBatchLoadProperties(),
    val blockPublish: BlockPublishProperties = BlockPublishProperties(),
    val eventConsume: EventConsumeProperties = EventConsumeProperties()
)

