-- TRANSACTIONS TABLE

CREATE TABLE dex_trades
(
    program_id VARCHAR(44) CODEC(LZ4),
    signature VARCHAR(88) CODEC(LZ4),
    block_slot Float64,
    block_time Float64,
    block_time_date DateTime,
    signer VARCHAR(44) CODEC(LZ4),
    in_mint VARCHAR(44) CODEC(LZ4),
    out_mint VARCHAR(44) CODEC(LZ4),
    in_amount Float64,
    out_amount Float64,
    txn_fee Float64,
    signer_sol_change Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(block_time)
PRIMARY KEY (signer, block_time, signature)
ORDER BY (signer, block_time, signature, in_mint, out_amount)
SETTINGS index_granularity = 8192;
