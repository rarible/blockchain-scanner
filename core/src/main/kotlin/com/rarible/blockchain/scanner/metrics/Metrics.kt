package com.rarible.blockchain.scanner.metrics

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit

@Component
class Metrics(
    @Autowired(required = false) val micrometer: MeterRegistry?
) {
    fun timed(name: String, vararg tags: Tag) = CoTimer(
        micrometer?.let {
            CoTimerConfig(
                it, Timer.builder(name)
                    .tags(tags.asList())
                    .register(it)
            )
        }
    )
}

data class CoTimerConfig(
    val micrometer: MeterRegistry,
    val timer: Timer
)

class CoTimer(private val config: CoTimerConfig?) {
    suspend fun <T> record(f: suspend () -> T): T {
        return if (config != null) {
            val start = config.micrometer.config().clock().monotonicTime()
            try {
                return f()
            } finally {
                val end = config.micrometer.config().clock().monotonicTime()
                config.timer.record(end - start, TimeUnit.NANOSECONDS)
            }
        } else {
            f()
        }
    }
}


