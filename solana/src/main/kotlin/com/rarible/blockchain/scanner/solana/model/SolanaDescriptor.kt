package com.rarible.blockchain.scanner.solana.model

import com.rarible.blockchain.scanner.framework.model.Descriptor

abstract class SolanaDescriptor(
    val filter: SolanaInstructionFilter,
    override val id: String,
    override val groupId: String,
    override val storage: SolanaLogStorage
) : Descriptor<SolanaLogStorage>

abstract class ProgramIdSolanaDescriptor(
    programId: String,
    id: String,
    groupId: String,
    storage: SolanaLogStorage
) : SolanaDescriptor(
    filter = SolanaInstructionFilter.ByProgramId(programId),
    id = id,
    groupId = groupId,
    storage = storage
)

abstract class DiscriminatorSolanaDescriptor(
    discriminator: ByteArray,
    id: String,
    groupId: String,
    storage: SolanaLogStorage
) : SolanaDescriptor(
    filter = SolanaInstructionFilter.ByDiscriminator(discriminator),
    id = id,
    groupId = groupId,
    storage = storage
)
