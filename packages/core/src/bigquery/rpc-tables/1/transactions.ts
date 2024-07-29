const query = `
SELECT
    access_list AS accessList,
    block_hash AS blockHash,
    tobase16_int64(block_number) AS blockNumber,
    from_address AS \`from\`,
    tobase16_int64(gas) AS gas,
    tobase16_int64(gas_price) AS gasPrice,
    transaction_hash AS \`hash\`,
    input,
    tobase16_int64(max_fee_per_gas) AS maxFeePerGas,
    tobase16_int64(max_priority_fee_per_gas) AS maxPriorityFeePerGas,
    tobase16_int64(nonce) AS nonce,
    r,
    s,
    to_address AS \`to\`,
    tobase16_int64(transaction_index) AS transactionIndex,
    tobase16_int64(transaction_type) AS \`type\`,
    tobase16_bignumeric(value) AS value,
    v
FROM
    \`bigquery-public-data.goog_blockchain_ethereum_mainnet_us.transactions\`
`;

export default query;
