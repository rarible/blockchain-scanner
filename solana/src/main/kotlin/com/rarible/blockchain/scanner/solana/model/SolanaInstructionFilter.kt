package com.rarible.blockchain.scanner.solana.model

import com.rarible.blockchain.scanner.solana.client.SolanaInstruction
import com.rarible.blockchain.scanner.solana.util.Base58

sealed interface SolanaInstructionFilter {
    fun matches(instruction: SolanaInstruction): Boolean

    data class ByProgramId(val programId: String) : SolanaInstructionFilter {
        override fun matches(instruction: SolanaInstruction): Boolean {
            return instruction.programId == programId
        }
    }

    @Suppress("ArrayInDataClass")
    data class ByDiscriminator(val discriminator: ByteArray) : SolanaInstructionFilter {
        private val discriminatorBase58 = Base58.encode(discriminator)
        override fun matches(instruction: SolanaInstruction): Boolean {
            return instruction.data.startsWith(discriminatorBase58)
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
