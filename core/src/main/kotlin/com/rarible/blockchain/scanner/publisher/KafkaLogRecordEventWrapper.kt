package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent

/**
 * Intercepts [LogRecordEvent] before it is serialized into JSON.
 *
 * Allows to provide a blockchain-specific customized subclass for the [LogRecordEvent],
 * which is serialized into JSON by standard Jackson means.
 * By providing a specific subclass the resulting JSON may contain specific type-discriminating fields,
 * which simplify deserialization.
 */
interface KafkaLogRecordEventWrapper<E> {

    val targetClass: Class<E>

    fun wrap(logRecordEvent: LogRecordEvent): E
}