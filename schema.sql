-- TRANSACTIONS TABLE

CREATE TABLE dex_trades
(
    program_id VARCHAR(44) CODEC(LZ4),
    signature VARCHAR(88) CODEC(LZ4),
    block_slot UInt64,
    block_time Int64,
    signer VARCHAR(44) CODEC(LZ4),
    in_mint VARCHAR(44) CODEC(LZ4),
    out_mint VARCHAR(44) CODEC(LZ4),
    in_amount Float64,
    out_amount Float64,
    txn_fee UInt64,
    signer_sol_change Int64
)
ENGINE = MergeTree
PRIMARY KEY (signer, block_time, signature)
ORDER BY (signer, block_time, signature, in_mint, out_amount)
SETTINGS index_granularity = 8192;
