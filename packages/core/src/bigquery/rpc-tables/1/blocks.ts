const query = `SELECT
    tobase16_int64(base_fee_per_gas) AS baseFeePerGas,
    tobase16_bignumeric(difficulty) AS difficulty,
    extra_data AS extraData,
    tobase16_int64(gas_limit) AS gasLimit,
    tobase16_int64(gas_used) AS gasUsed,
    block_hash AS \`hash\`,
    logs_bloom AS logsBloom,
    miner,
    mix_hash AS mixHash,
    nonce,
    tobase16_int64(block_number) AS number,
    parent_hash AS parentHash,
    receipts_root AS receiptsRoot,
    sha3_uncles AS sha3Uncles,
    tobase16_int64(size) AS size,
    state_root AS stateRoot,
    tobase16_int64(UNIX_SECONDS(block_timestamp)) AS timestamp,
    tobase16_bignumeric(total_difficulty) AS totalDifficulty,
    transactions_root AS transactionsRoot
FROM
    \`bigquery-public-data.goog_blockchain_ethereum_mainnet_us.blocks\`
`;

export default query;
