package com.rarible.blockchain.scanner.framework.listener

class SimpleLogRecordEventListener(
    override val id: String,
    override val groupId: String,
    subscribers: List<LogRecordEventSubscriber>
) : AbstractLogRecordEventListener(subscribers)