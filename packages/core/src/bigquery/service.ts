import * as fs from "node:fs";
import * as path from "node:path";
import type {
  JobMetadata,
  JobMetadataResponse,
  JobResponse,
} from "@google-cloud/bigquery";
import { BigQuery } from "@google-cloud/bigquery";
import { Storage } from "@google-cloud/storage";

import type {
  BlockSource,
  CallTraceSource,
  FactoryCallTraceSource,
  FactoryLogSource,
  LogFilterCriteria,
  LogSource,
} from "@/config/sources.js";
import type { SyncBlock } from "@/sync/index.js";
import { hash } from "@/utils/hash.js";

const DATABASE_MAP: Record<number, string> = {
  10: "bigquery-public-data.goog_blockchain_optimism_mainnet_us",
  1: "bigquery-public-data.goog_blockchain_ethereum_mainnet_us",
};

class BigQueryService {
  private bigquery: BigQuery;
  private storage: Storage;
  private projectId: string;
  private tempDatasetId: string;
  private bucketName: string;
  private filename: string;

  constructor(
    projectId: string,
    tempDatasetId: string,
    bucketName: string,
    filename: string,
  ) {
    this.bigquery = new BigQuery({ projectId });
    this.storage = new Storage({ projectId });
    this.projectId = projectId;
    this.tempDatasetId = tempDatasetId;
    this.bucketName = bucketName;
    this.filename = filename;
  }

  private async waitForJobCompletion(
    jobId: string,
    extractUUID = false,
  ): Promise<void> {
    const finalJobId = extractUUID ? jobId.split(".").pop() : jobId;
    if (!finalJobId) {
      throw new Error("Invalid job ID format");
    }
    const job = this.bigquery.job(finalJobId);

    return new Promise((resolve, reject) => {
      job.on("complete", (metadata) => {
        console.log(`Job ${finalJobId} completed.`);
        resolve(metadata);
      });

      job.on("error", (err) => {
        console.error(`Job ${finalJobId} failed:`, err);
        reject(err);
      });
    });
  }

  async extractTableToGCS(tableId: string): Promise<void> {
    const options: {
      format?: "CSV" | "JSON" | "AVRO" | "PARQUET" | "ORC";
      gzip?: boolean;
    } = {
      format: "JSON",
      gzip: false,
    };

    const [job]: JobMetadataResponse = await this.bigquery
      .dataset(this.tempDatasetId)
      .table(tableId)
      .extract(
        this.storage
          .bucket(this.bucketName)
          .file(`${this.filename}/${tableId}/*`),
        options,
      );

    console.log(`Job ${job.id} created to export table ${tableId} to GCS.`);

    if (job.id == null) {
      throw new Error("Job id is not defined");
    }
    await this.waitForJobCompletion(job.id, true);
  }

  private async ddl(query: string): Promise<JobMetadata> {
    const options = {
      query: query,
    };

    const [job]: JobResponse = await this.bigquery.createQueryJob(options);

    console.log(`DDL Job ${job.id} created for query execution.`);

    if (job.id == null) {
      throw new Error("Job id is not defined");
    }
    await this.waitForJobCompletion(job.id);
    const [metadata] = await job.getMetadata();
    return metadata;
  }

  async tableExists(datasetId: string, tableId: string): Promise<boolean> {
    const [tables] = await this.bigquery.dataset(datasetId).getTables();
    return tables.some((table) => table.id === tableId);
  }

  async directoryExists(prefix: string): Promise<boolean> {
    const [files] = await this.storage
      .bucket(this.bucketName)
      .getFiles({ prefix });
    return files.length > 0;
  }

  async listFilesInDirectory(prefix: string): Promise<string[]> {
    const [files] = await this.storage
      .bucket(this.bucketName)
      .getFiles({ prefix });
    return files.map((file) => file.name);
  }

  async downloadFileFromGCS(
    srcFilename: string,
    destFilename: string,
  ): Promise<void> {
    const destDir = path.dirname(destFilename);

    if (!fs.existsSync(destDir)) {
      fs.mkdirSync(destDir, { recursive: true });
      console.log(`Directory ${destDir} created.`);
    }

    const options = {
      destination: destFilename,
    };
    await this.storage
      .bucket(this.bucketName)
      .file(srcFilename)
      .download(options);
    console.log(`File ${srcFilename} downloaded to ${destFilename}.`);
  }

  async exportSourceToGCS(
    source:
      | LogSource
      | FactoryLogSource
      | CallTraceSource
      | FactoryCallTraceSource
      | BlockSource,
    startBlock: SyncBlock,
    endBlock: SyncBlock,
  ): Promise<string | undefined> {
    const sourceDatabase = DATABASE_MAP[source.chainId];
    if (sourceDatabase) {
      let whereClause = "1 = 1";

      if (source.type === "log") {
        const criteria = source.criteria as LogFilterCriteria;

        if (criteria.address) {
          if (Array.isArray(criteria.address)) {
            whereClause += ` AND lower(address) IN (${criteria.address.map((address) => `lower('${address}')`).join(", ")})`;
          } else {
            whereClause += ` AND lower(address) = lower('${criteria.address}')`;
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
          whereClause += ` AND topics[ORDINAL(1)] IN (${topics.map((t) => `'${t}'`).join(", ")})`;
        }

        whereClause += ` AND block_number >= ${Number.parseInt(startBlock.number)} AND block_number <= ${Number.parseInt(endBlock.number)}`;
        whereClause += ` AND block_timestamp >= TIMESTAMP_SECONDS(${Number.parseInt(startBlock.timestamp)}) AND block_timestamp <= TIMESTAMP_SECONDS(${Number.parseInt(endBlock.timestamp)})`;
      }

      const tableId = `temp_${hash(JSON.stringify(source))}`;
      const tableAlreadyExists = await this.tableExists(
        this.tempDatasetId,
        tableId,
      );
      const directoryAlreadyExists = await this.directoryExists(
        `${this.filename}/${tableId}`,
      );

      if (!tableAlreadyExists) {
        const query = `
        CREATE TEMP FUNCTION tobase16_int64(x INT64)
        RETURNS STRING
        LANGUAGE js AS """
          if (x === null) return null;
          return '0x' + Number(x).toString(16);
        """;

        CREATE TEMP FUNCTION tobase16_bignumeric(x BIGNUMERIC)
        RETURNS STRING
        LANGUAGE js AS """
          if (x === null) return null;
          return '0x' + BigInt(x).toString(16);
        """;
        
        CREATE TABLE IF NOT EXISTS \`${this.projectId}.${this.tempDatasetId}.${tableId}\` AS
        WITH 
        logs_hex AS (
          SELECT
            address,
            block_hash AS blockHash,
            tobase16_int64(block_number) AS blockNumber,
            data,
            CONCAT(block_hash, '-', tobase16_int64(log_index)) AS id,
            tobase16_int64(log_index) AS logIndex,
            topics[SAFE_OFFSET(0)] AS topic0,
            topics[SAFE_OFFSET(1)] AS topic1,
            topics[SAFE_OFFSET(2)] AS topic2,
            topics[SAFE_OFFSET(3)] AS topic3,
            transaction_hash AS transactionHash,
            tobase16_int64(transaction_index) AS transactionIndex
          FROM
            \`${sourceDatabase}.logs\`
          WHERE
            ${whereClause}
        ),

        transactions_hex AS (
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
            \`${sourceDatabase}.transactions\`
          WHERE
            block_number BETWEEN ${Number.parseInt(startBlock.number)} AND ${Number.parseInt(endBlock.number)}
            AND block_timestamp BETWEEN TIMESTAMP_SECONDS(${Number.parseInt(startBlock.timestamp)}) AND TIMESTAMP_SECONDS(${Number.parseInt(endBlock.timestamp)})
        ),

        receipts_hex AS (
          SELECT
            block_hash AS blockHash,
            tobase16_int64(block_number) AS blockNumber,
            contract_address AS contractAddress,
            tobase16_int64(cumulative_gas_used) AS cumulativeGasUsed,
            tobase16_int64(effective_gas_price) AS effectiveGasPrice,
            from_address AS \`from\`,
            tobase16_int64(gas_used) AS gasUsed,
            logs_bloom AS logsBloom,
            tobase16_int64(status) AS status,
            to_address AS \`to\`,
            transaction_hash AS transactionHash,
            tobase16_int64(transaction_index) AS transactionIndex,
            CAST(NULL AS STRING) AS \`type\`
          FROM
            \`${sourceDatabase}.receipts\`
          WHERE
            block_number BETWEEN ${Number.parseInt(startBlock.number)} AND ${Number.parseInt(endBlock.number)}
            AND block_timestamp BETWEEN TIMESTAMP_SECONDS(${Number.parseInt(startBlock.timestamp)}) AND TIMESTAMP_SECONDS(${Number.parseInt(endBlock.timestamp)})
        ),

        blocks_hex AS (
          SELECT
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
            CAST(block_timestamp AS STRING) AS timestamp,
            tobase16_bignumeric(total_difficulty) AS totalDifficulty,
            transactions_root AS transactionsRoot
          FROM
            \`${sourceDatabase}.blocks\`
          WHERE
            block_number BETWEEN ${Number.parseInt(startBlock.number)} AND ${Number.parseInt(endBlock.number)}
            AND block_timestamp BETWEEN TIMESTAMP_SECONDS(${Number.parseInt(startBlock.timestamp)}) AND TIMESTAMP_SECONDS(${Number.parseInt(endBlock.timestamp)})
        ),

        logs_json AS (
          SELECT
            blockHash,
            blockNumber,
            ARRAY_AGG(logs_hex) AS logs
          FROM
            logs_hex
          GROUP BY
            blockHash, blockNumber
        ),

        transactions_json AS (
          SELECT
            blockHash,
            blockNumber,
            ARRAY_AGG(transactions_hex) AS transactions
          FROM
            transactions_hex
          GROUP BY
            blockHash, blockNumber
        ),

        receipts_json AS (
          SELECT
            blockHash,
            blockNumber,
            ARRAY_AGG(receipts_hex) AS receipts
          FROM
            receipts_hex
          GROUP BY
            blockHash, blockNumber
        )

        SELECT
          blocks_hex.*,
          transactions_json.transactions,
          logs_json.logs,
          receipts_json.receipts
        FROM
          logs_json
        JOIN
          transactions_json USING (blockHash, blockNumber)
        JOIN
          blocks_hex ON blocks_hex.hash = logs_json.blockHash AND blocks_hex.number = logs_json.blockNumber
        JOIN
          receipts_json USING (blockHash, blockNumber)
      `;

        const metadata = await this.ddl(query);
        console.log(`Created new table ${tableId} via job ${metadata.id}`);
      } else {
        console.log(`Table ${tableId} already exists. Skipping creation.`);
      }

      if (!directoryAlreadyExists) {
        await this.extractTableToGCS(tableId);
        console.log(
          `Exported table ${tableId} to ${this.bucketName}/${this.filename}`,
        );
      } else {
        console.log(
          `Directory ${this.filename}/${tableId} already exists in bucket ${this.bucketName}. Skipping extraction.`,
        );
      }

      return `${this.filename}/${tableId}`;
    } else {
      console.warn(`Source database for chainId ${source.chainId} not found.`);
    }
    return;
  }

  isChainIdWhitelisted(chainId: number): boolean {
    return chainId in DATABASE_MAP;
  }
}

export { BigQueryService };
