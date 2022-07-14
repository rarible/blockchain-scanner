package com.rarible.blockchain.scanner.solana.client

data class SolanaInstruction(
    val programId: String,
    val data: String,
    val accounts: List<String>
) {
    override fun toString(): String = buildString {
        appendLine("Program ID: $programId")
        appendLine("Data: $data")
        appendLine("Accounts:")
        accounts.forEachIndexed { index, account ->
            appendLine("    ${index + 1}. $account")
        }
    }
}
