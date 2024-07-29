import * as fs from "node:fs";
import * as path from "node:path";
import type {
  JobMetadata,
  JobMetadataResponse,
  JobResponse,
} from "@google-cloud/bigquery";
import { BigQuery } from "@google-cloud/bigquery";
import { Storage } from "@google-cloud/storage";

import type { EventSource, LogSource } from "@/config/sources.js";
import type { SyncBlock } from "@/sync/index.js";
import { hash } from "@/utils/hash.js";
import { generateLogSourceQuery } from "./querygen.js";

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

  async sourceWhitelisted(source: EventSource): Promise<boolean> {
    if (!fs.existsSync(`/rpc-tables/${source.chainId}`)) {
      if (source.type in ["log"]) {
        return true;
      }
    }
    return false;
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

  async exportLogSourceToGCS(
    source: LogSource,
    startBlock: SyncBlock,
    endBlock: SyncBlock,
  ): Promise<string | undefined> {
    const query = await generateLogSourceQuery(
      source,
      startBlock,
      endBlock,
      this.tempDatasetId,
      this.projectId,
    );

    if (!query) {
      console.error("Failed to generate query for log source.");
      return;
    }

    const tableId = `temp_${hash(JSON.stringify(source))}`;

    const tableAlreadyExists = await this.tableExists(
      this.tempDatasetId,
      tableId,
    );
    const directoryAlreadyExists = await this.directoryExists(
      `${this.filename}/${tableId}`,
    );

    if (!directoryAlreadyExists) {
      if (!tableAlreadyExists) {
        const metadata = await this.ddl(query);
        console.log(`Created new table ${tableId} via job ${metadata.id}`);
      } else {
        console.log(`Table ${tableId} already exists. Skipping creation.`);
      }
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
  }
}

export { BigQueryService };
