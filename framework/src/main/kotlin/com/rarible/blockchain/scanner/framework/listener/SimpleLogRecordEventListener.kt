package com.rarible.blockchain.scanner.framework.listener

class SimpleLogRecordEventListener(
    id: String,
    groupId: String,
    subscribers: List<LogRecordEventSubscriber>
) : AbstractLogRecordEventListener(id = id, groupId = groupId, subscribers)