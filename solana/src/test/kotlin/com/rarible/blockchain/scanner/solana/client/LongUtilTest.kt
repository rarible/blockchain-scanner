package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.solana.util.toFixedLengthString
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class LongUtilTest {
    @Test
    fun `to fixed length string`() {
        assertThat(0L.toFixedLengthString(2)).isEqualTo("00")
        assertThat(1L.toFixedLengthString(4)).isEqualTo("0001")
        assertThat(10L.toFixedLengthString(4)).isEqualTo("0010")
        assertThat(16L.toFixedLengthString(4)).isEqualTo("0016")
        assertThat(1_000_000_000L.toFixedLengthString(12)).isEqualTo("001000000000")
        assertThrows<Exception> { 123456789L.toFixedLengthString(2) }
    }
}
