const query = `
SELECT
    t.access_list AS accessList,
    t.block_hash AS blockHash,
    tobase16_int64(b.block_number) AS blockNumber,
    t.from_address AS \`from\`,
    tobase16_int64(t.gas) AS gas,
    tobase16_bignumeric(t.gas_price.bignumeric_value) AS gasPrice,
    t.transaction_hash AS \`hash\`,
    t.input,
    tobase16_int64(t.max_fee_per_gas) AS maxFeePerGas,
    tobase16_int64(t.max_priority_fee_per_gas) AS maxPriorityFeePerGas,
    tobase16_bignumeric(t.nonce) AS nonce,
    tobase16_string(t.r.string_value) AS r,
    tobase16_string(t.s.string_value) AS s,
    t.to_address AS \`to\`,
    tobase16_int64(t.transaction_index) AS transactionIndex,
    tobase16_int64(t.transaction_type) AS \`type\`,
    tobase16_bignumeric(t.value.bignumeric_value) AS value,
    tobase16_string(t.v.string_value) AS v
FROM
    \`bigquery-public-data.goog_blockchain_optimism_mainnet_us.transactions\` t
JOIN \`bigquery-public-data.goog_blockchain_optimism_mainnet_us.blocks\` b USING (block_hash, block_timestamp)
`;
export default query;
