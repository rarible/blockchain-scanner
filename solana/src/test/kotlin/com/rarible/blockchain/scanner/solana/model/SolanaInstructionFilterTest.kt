package com.rarible.blockchain.scanner.solana.model

import com.rarible.blockchain.scanner.solana.client.SolanaInstruction
import com.rarible.blockchain.scanner.solana.util.Base58
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class SolanaInstructionFilterTest {

    @Test
    fun `test ByProgramId filter`() {
        val programId = "testProgramId"
        val filter = SolanaInstructionFilter.ByProgramId(programId)

        assertThat(filter.matches(SolanaInstruction(programId = programId, data = "data", accounts = emptyList()))).isTrue()
        assertThat(filter.matches(SolanaInstruction(programId = "other", data = "data", accounts = emptyList()))).isFalse()
    }

    @Test
    fun `test ByDiscriminator filter`() {
        val discriminator = "test".toByteArray()
        val filter = SolanaInstructionFilter.ByDiscriminator(discriminator)

        val matchingData = Base58.encode("test123".toByteArray())
        val nonMatchingData = Base58.encode("other".toByteArray())

        assertThat(filter.matches(SolanaInstruction(programId = "id1", data = matchingData, accounts = emptyList()))).isTrue()
        assertThat(filter.matches(SolanaInstruction(programId = "id2", data = nonMatchingData, accounts = emptyList()))).isFalse()
    }

    @Test
    fun `test And filter`() {
        val programId = "testProgramId"
        val discriminator = "test".toByteArray()

        val filter = SolanaInstructionFilter.And(
            SolanaInstructionFilter.ByProgramId(programId),
            SolanaInstructionFilter.ByDiscriminator(discriminator)
        )

        val matchingInstruction = SolanaInstruction(programId = programId, data = Base58.encode("test".toByteArray()), accounts = emptyList())
        val nonMatchingInstruction = SolanaInstruction(programId = "other", data = Base58.encode("test".toByteArray()), accounts = emptyList())

        assertThat(filter.matches(matchingInstruction)).isTrue()
        assertThat(filter.matches(nonMatchingInstruction)).isFalse()
    }

    @Test
    fun `test Or filter`() {
        val programId1 = "testProgramId1"
        val programId2 = "testProgramId2"

        val filter = SolanaInstructionFilter.Or(
            SolanaInstructionFilter.ByProgramId(programId1),
            SolanaInstructionFilter.ByProgramId(programId2)
        )

        assertThat(filter.matches(SolanaInstruction(programId = programId1, data = "data", accounts = emptyList()))).isTrue()
        assertThat(filter.matches(SolanaInstruction(programId = programId2, data = "data", accounts = emptyList()))).isTrue()
        assertThat(filter.matches(SolanaInstruction(programId = "other", data = "data", accounts = emptyList()))).isFalse()
    }

    @Test
    fun `test True filter`() {
        val filter = SolanaInstructionFilter.True

        assertThat(filter.matches(SolanaInstruction(programId = "any", data = "data", accounts = emptyList()))).isTrue()
    }
}
