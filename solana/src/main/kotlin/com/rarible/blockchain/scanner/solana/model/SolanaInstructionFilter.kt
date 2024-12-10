package com.rarible.blockchain.scanner.solana.model

import com.rarible.blockchain.scanner.solana.client.SolanaInstruction
import com.rarible.blockchain.scanner.solana.util.Base58

interface SolanaInstructionFilter {
    fun matches(instruction: SolanaInstruction): Boolean

    data class ByProgramId(val programId: String) : SolanaInstructionFilter {
        override fun matches(instruction: SolanaInstruction): Boolean {
            return instruction.programId == programId
        }
    }

    data class ByProgramIds(val programIds: Collection<String>) : SolanaInstructionFilter {
        constructor(vararg programId: String) : this(programId.toSet())

        private val programIdsSet = programIds.toSet()

        override fun matches(instruction: SolanaInstruction): Boolean {
            return instruction.programId in programIdsSet
        }
    }

    @Suppress("ArrayInDataClass")
    data class ByDiscriminator(val discriminator: ByteArray) : SolanaInstructionFilter {
        override fun matches(instruction: SolanaInstruction): Boolean {
            val data = Base58.decode(instruction.data)
            return data.size >= discriminator.size && data.sliceArray(discriminator.indices).contentEquals(discriminator)
        }
    }

    data class And(val filters: Collection<SolanaInstructionFilter>) : SolanaInstructionFilter {
        constructor(vararg filters: SolanaInstructionFilter) : this(filters.toSet())

        override fun matches(instruction: SolanaInstruction): Boolean {
            return filters.all { it.matches(instruction) }
        }
    }

    data class Or(val filters: Collection<SolanaInstructionFilter>) : SolanaInstructionFilter {
        constructor(vararg filters: SolanaInstructionFilter) : this(filters.toSet())

        override fun matches(instruction: SolanaInstruction): Boolean =
            filters.any { it.matches(instruction) }
    }

    object True : SolanaInstructionFilter {
        override fun matches(instruction: SolanaInstruction): Boolean = true
    }
}
