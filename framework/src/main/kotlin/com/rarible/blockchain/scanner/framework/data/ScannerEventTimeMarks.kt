package com.rarible.blockchain.scanner.framework.data

import java.time.Instant

data class ScannerEventTimeMarks(
    val marks: List<ScannerSourceEventTimeMark> = emptyList()
) {

    companion object {

        fun of(instant: Instant): ScannerEventTimeMarks {
            return ScannerEventTimeMarks(
                listOf(
                    ScannerSourceEventTimeMark("source", instant),
                    ScannerSourceEventTimeMark("scanner-in", Instant.now()),
                )
            )
        }
    }

    fun addOut() = add("scanner-out")

    private fun add(name: String, date: Instant? = null): ScannerEventTimeMarks {
        val mark = ScannerSourceEventTimeMark(name, date ?: Instant.now())
        val marks = this.marks + mark
        return this.copy(marks = marks)
    }

}

data class ScannerSourceEventTimeMark(
    val name: String,
    val date: Instant = Instant.now(),
)