const query = `
SELECT
    address,
    block_hash AS blockHash,
    tobase16_int64(block_number) AS blockNumber,
    data,
    CONCAT(block_hash, '-', tobase16_int64(log_index)) AS id,
    tobase16_int64(log_index) AS logIndex,
    topics,
    transaction_hash AS transactionHash,
    tobase16_int64(transaction_index) AS transactionIndex
FROM
  \`bigquery-public-data.goog_blockchain_optimism_mainnet_us.logs\`
`;
export default query;
