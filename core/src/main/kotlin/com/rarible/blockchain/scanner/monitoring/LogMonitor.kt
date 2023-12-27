package com.rarible.blockchain.scanner.monitoring

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.ImmutableTag
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass

class LogMonitor(
    properties: BlockchainScannerProperties,
    meterRegistry: MeterRegistry
) : AbstractMonitor(
    properties,
    meterRegistry,
    "log"
) {
    private val logCounters = ConcurrentHashMap<String, Counter>()

    override fun register() = Unit

    private fun getInsertedLogsCounter(descriptor: Descriptor): Counter =
        logCounters.getOrPut(descriptor.id) {
            addCounter(
                metricName = INSERTED_LOGS,
                Tag.of("subscriber", descriptor.alias ?: descriptor.id)
            )
        }

    fun onLogsInserted(descriptor: Descriptor, inserted: Int) {
        getInsertedLogsCounter(descriptor).increment(inserted.toDouble())
    }

    inline fun <T> onPrepareLogs(block: () -> T): T {
        return recordTime(getTimer(PREPARE_LOGS), block)
    }

    inline fun <T> onSaveLogs(block: () -> T): T {
        return recordTime(getTimer(SAVE_LOGS), block)
    }

    inline fun <T> onPublishLogs(block: () -> T): T {
        return recordTime(getTimer(PUBLISH_LOGS), block)
    }

    inline fun <T> onGetEventRecords(subscriber: KClass<out LogEventSubscriber<*, *, *, *>>, handler: () -> T): T {
        return recordTime(
            getTimer(
                SUBSCRIBER_RECORDS,
                ImmutableTag("subscriber", subscriber.simpleName ?: "")
            ), handler
        )
    }

    inline fun <T> onPostProcess(subscriber: KClass<out LogEventSubscriber<*, *, *, *>>, handler: () -> T): T {
        return recordTime(
            getTimer(
                SUBSCRIBER_POST_PROCESS,
                ImmutableTag("subscriber", subscriber.simpleName ?: "")
            ), handler
        )
    }

    companion object {
        const val INSERTED_LOGS = "inserted_logs"
        const val PREPARE_LOGS = "prepare_logs"
        const val SAVE_LOGS = "save_logs"
        const val PUBLISH_LOGS = "publish_logs"
        const val SUBSCRIBER_RECORDS = "subscriber_get_event_records"
        const val SUBSCRIBER_POST_PROCESS = "subscriber_post_process"
    }
}
