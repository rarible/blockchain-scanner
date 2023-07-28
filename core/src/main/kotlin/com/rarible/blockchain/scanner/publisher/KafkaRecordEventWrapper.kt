package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.framework.data.RecordEvent
import com.rarible.blockchain.scanner.framework.model.Record

/**
 * Intercepts [RecordEvent] before it is serialized into JSON.
 *
 * Allows to provide a blockchain-specific customized subclass for the [LogRecordEvent],
 * which is serialized into JSON by standard Jackson means.
 * By providing a specific subclass the resulting JSON may contain specific type-discriminating fields,
 * which simplify deserialization.
 */
interface KafkaRecordEventWrapper<E, R : Record, RE : RecordEvent<R>> {

    val targetClass: Class<E>

    fun wrap(logRecordEvent: RE): E
}
