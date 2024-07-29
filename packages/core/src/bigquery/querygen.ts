import type { LogFilterCriteria, LogSource } from "@/config/sources.js";
import type { SyncBlock } from "@/sync/index.js";
import { hash } from "@/utils/hash.js";
import { queries } from "./rpc-tables/index.js";

const TEMP_FUNCTIONS = [
  `
    CREATE TEMP FUNCTION tobase16_string(x STRING)
    RETURNS STRING
    LANGUAGE js AS """
    if (x === null) return null;
    return '0x' + BigInt(x).toString(16);
    """;
  `,
  `
    CREATE TEMP FUNCTION tobase16_int64(x INT64)
    RETURNS STRING
    LANGUAGE js AS """
    if (x === null) return null;
    return '0x' + Number(x).toString(16);
    """;
  `,
  `
    CREATE TEMP FUNCTION tobase16_bignumeric(x BIGNUMERIC)
    RETURNS STRING
    LANGUAGE js AS """
    if (x === null) return null;
    return '0x' + BigInt(x).toString(16);
    """;
  `,
];

export async function generateLogSourceQuery(
  source: LogSource,
  startBlock: SyncBlock,
  endBlock: SyncBlock,
  tempDatasetId: string,
  projectId: string,
): Promise<string | undefined> {
  const criteria = source.criteria as LogFilterCriteria;

  if (criteria.includeTransactionReceipts) {
    throw new Error("Transaction receipts are not supported for log sources.");
  }

  let query = `${TEMP_FUNCTIONS.join("\n")}\n`;

  const tableId = `temp_${hash(JSON.stringify(source))}`;

  query += `CREATE TABLE IF NOT EXISTS \`${projectId}.${tempDatasetId}.${tableId}\` AS\n`;

  query += "WITH\n";

  const cteQueries = [];

  for (const table of ["logs", "transactions", "blocks"]) {
    // Read table file from directory rpc-tables/{chainId}/{table}.sql
    // If file does not exist, return undefined
    let body: string;
    try {
      body = queries[`${table}_${source.chainId}`];
    } catch (err) {
      return;
    }

    let cteQuery = `\n${table} AS (\n`;

    cteQuery += body;

    cteQuery += "\nWHERE 1 = 1\n";

    if (table === "logs") {
      if (criteria.address) {
        if (Array.isArray(criteria.address)) {
          cteQuery += `\nAND lower(address) IN (${criteria.address.map((address) => `lower('${address}')`).join(", ")})`;
        } else {
          cteQuery += `\nAND lower(address) = lower('${criteria.address}')`;
        }
      }

      if (criteria.topics.length > 0) {
        const topics: `0x${string}`[] = [];
        for (const topic of criteria.topics) {
          if (Array.isArray(topic)) {
            topics.push(...topic);
          } else if (topic !== null) {
            topics.push(topic);
          }
        }
        cteQuery += `\nAND topics[ORDINAL(1)] IN (${topics.map((t) => `'${t}'`).join(", ")})`;
      }
    }
    cteQuery += `\nAND block_number >= ${Number.parseInt(startBlock.number)} AND block_number <= ${Number.parseInt(endBlock.number)}`;
    cteQuery += `\nAND block_timestamp >= TIMESTAMP_SECONDS(${Number.parseInt(startBlock.timestamp)}) AND block_timestamp <= TIMESTAMP_SECONDS(${Number.parseInt(endBlock.timestamp)})`;

    cteQuery += "\n)";
    cteQueries.push(cteQuery);
  }

  query += cteQueries.join(",\n");

  query += `,
  logs_agg AS (
    SELECT
      blockHash,
      blockNumber,
      transactionHash,
      transactionIndex,
      ARRAY_AGG(t) AS logs
    FROM
      logs t
    GROUP BY
      blockHash, blockNumber, transactionHash, transactionIndex
  ),

  transactions_agg AS (
    SELECT
      blockHash,
      blockNumber,
      \`hash\` AS transactionHash,
      transactionIndex,
      ARRAY_AGG(t) AS transactions
    FROM
      transactions t
    GROUP BY
      blockHash, blockNumber, \`hash\`, transactionIndex
  )

  SELECT
    blocks.*,
    transactions_agg.transactions,
    logs_agg.logs,
    [] AS transactionReceipts
  FROM
    logs_agg
  JOIN
    transactions_agg USING (blockHash, blockNumber, transactionHash, transactionIndex)
  JOIN
    blocks ON blocks.hash = logs_agg.blockHash AND blocks.number = logs_agg.blockNumber
  `;

  return query;
}
