import {
  type BlockFilterCriteria,
  type CallTraceFilterCriteria,
  type CallTraceSource,
  type EventSource,
  type FactoryCallTraceFilterCriteria,
  type FactoryCallTraceSource,
  type FactoryLogFilterCriteria,
  type FactoryLogSource,
  type LogFilterCriteria,
  type LogSource,
  sourceIsBlock,
  sourceIsCallTrace,
  sourceIsFactoryCallTrace,
  sourceIsFactoryLog,
  sourceIsLog,
} from "@/config/sources.js";
import type { HeadlessKysely } from "@/database/kysely.js";
import type { SyncCallTrace, SyncLog } from "@/sync/index.js";
import type { CallTrace, Log, TransactionReceipt } from "@/types/eth.js";
import type { NonNull } from "@/types/utils.js";
import {
  type Checkpoint,
  EVENT_TYPES,
  decodeCheckpoint,
  encodeCheckpoint,
  maxCheckpoint,
  zeroCheckpoint,
} from "@/utils/checkpoint.js";
import {
  buildFactoryLogFragments,
  buildFactoryTraceFragments,
  buildLogFilterFragments,
  buildTraceFragments,
} from "@/utils/fragments.js";
import { intervalIntersectionMany, intervalUnion } from "@/utils/interval.js";
import { range } from "@/utils/range.js";
import {
  type ExpressionBuilder,
  type OperandExpression,
  type SqlBool,
  type Transaction as KyselyTransaction,
  sql,
} from "kysely";
import {
  type Hex,
  type RpcBlock,
  type RpcLog,
  type RpcTransaction,
  type RpcTransactionReceipt,
  checksumAddress,
  hexToBigInt,
  hexToNumber,
} from "viem";
import type { RawEvent, SyncStore } from "../store.js";
import {
  type InsertableCallTrace,
  type SyncStoreTables,
  rpcToPostgresBlock,
  rpcToPostgresLog,
  rpcToPostgresTrace,
  rpcToPostgresTransaction,
  rpcToPostgresTransactionReceipt,
} from "./encoding.js";

export class PostgresSyncStore implements SyncStore {
  kind = "postgres" as const;
  db: HeadlessKysely<SyncStoreTables>;

  constructor({ db }: { db: HeadlessKysely<SyncStoreTables> }) {
    this.db = db;
  }

  insertLogFilterInterval = async ({
    chainId,
    logFilter,
    block: rpcBlock,
    transactions: rpcTransactions,
    transactionReceipts: rpcTransactionReceipts,
    logs: rpcLogs,
    interval,
  }: {
    chainId: number;
    logFilter: LogFilterCriteria;
    block: RpcBlock;
    transactions: RpcTransaction[];
    transactionReceipts: RpcTransactionReceipt[];
    logs: RpcLog[];
    interval: { startBlock: bigint; endBlock: bigint };
  }) => {
    return this.db.wrap({ method: "insertLogFilterInterval" }, async () => {
      await this.db.transaction().execute(async (tx) => {
        await tx
          .insertInto("blocks")
          .values({
            ...rpcToPostgresBlock(rpcBlock),
            chainId,
            checkpoint: this.createBlockCheckpoint(rpcBlock, chainId),
          })
          .onConflict((oc) => oc.column("hash").doNothing())
          .execute();

        if (rpcTransactions.length > 0) {
          const transactions = rpcTransactions.map((transaction) => ({
            ...rpcToPostgresTransaction(transaction),
            chainId,
          }));
          await tx
            .insertInto("transactions")
            .values(transactions)
            .onConflict((oc) => oc.column("hash").doNothing())
            .execute();
        }

        if (rpcTransactionReceipts.length > 0) {
          const transactionReceipts = rpcTransactionReceipts.map(
            (rpcTransactionReceipt) => ({
              ...rpcToPostgresTransactionReceipt(rpcTransactionReceipt),
              chainId,
            }),
          );
          await tx
            .insertInto("transactionReceipts")
            .values(transactionReceipts)
            .onConflict((oc) => oc.column("transactionHash").doNothing())
            .execute();
        }

        if (rpcLogs.length > 0) {
          const logs = rpcLogs.map((rpcLog) => ({
            ...rpcToPostgresLog(rpcLog),
            chainId,
            checkpoint: this.createLogCheckpoint(rpcLog, rpcBlock, chainId),
          }));
          await tx
            .insertInto("logs")
            .values(logs)
            .onConflict((oc) =>
              oc.column("id").doUpdateSet((eb) => ({
                checkpoint: eb.ref("excluded.checkpoint"),
              })),
            )
            .execute();
        }

        await this._insertLogFilterInterval({
          tx,
          chainId,
          logFilters: [logFilter],
          interval,
        });
      });
    });
  };

  getLogFilterIntervals = async ({
    chainId,
    logFilter,
  }: {
    chainId: number;
    logFilter: LogFilterCriteria;
  }) => {
    return this.db.wrap({ method: "getLogFilterIntervals" }, async () => {
      const fragments = buildLogFilterFragments({ ...logFilter, chainId });

      // First, attempt to merge overlapping and adjacent intervals.
      await Promise.all(
        fragments.map(async (fragment) => {
          return await this.db.transaction().execute(async (tx) => {
            const { id: logFilterId } = await tx
              .insertInto("logFilters")
              .values(fragment)
              .onConflict((oc) => oc.column("id").doUpdateSet(fragment))
              .returningAll()
              .executeTakeFirstOrThrow();

            const existingIntervalRows = await tx
              .deleteFrom("logFilterIntervals")
              .where("logFilterId", "=", logFilterId)
              .returningAll()
              .execute();

            const mergedIntervals = intervalUnion(
              existingIntervalRows.map((i) => [
                Number(i.startBlock),
                Number(i.endBlock),
              ]),
            );

            const mergedIntervalRows = mergedIntervals.map(
              ([startBlock, endBlock]) => ({
                logFilterId,
                startBlock: BigInt(startBlock),
                endBlock: BigInt(endBlock),
              }),
            );

            if (mergedIntervalRows.length > 0) {
              await tx
                .insertInto("logFilterIntervals")
                .values(mergedIntervalRows)
                .execute();
            }
          });
        }),
      );

      const intervals = await this.db
        .with(
          "logFilterFragments(fragmentId, fragmentAddress, fragmentTopic0, fragmentTopic1, fragmentTopic2, fragmentTopic3, fragmentIncludeTransactionReceipts)",
          () =>
            sql`( values ${sql.join(
              fragments.map(
                (f) =>
                  sql`( ${sql.val(f.id)}, ${sql.val(f.address)}, ${sql.val(
                    f.topic0,
                  )}, ${sql.val(f.topic1)}, ${sql.val(f.topic2)}, ${sql.val(
                    f.topic3,
                  )}, ${sql.lit(f.includeTransactionReceipts)} )`,
              ),
            )} )`,
        )
        .selectFrom("logFilterIntervals")
        .innerJoin("logFilters", "logFilterId", "logFilters.id")
        .innerJoin("logFilterFragments", (join) => {
          let baseJoin = join.on((eb) =>
            eb.or([
              eb("address", "is", null),
              eb("fragmentAddress", "=", sql.ref("address")),
            ]),
          );
          baseJoin = baseJoin.on((eb) =>
            eb(
              "fragmentIncludeTransactionReceipts",
              "<=",
              sql.ref("includeTransactionReceipts"),
            ),
          );
          for (const idx_ of range(0, 4)) {
            baseJoin = baseJoin.on((eb) => {
              const idx = idx_ as 0 | 1 | 2 | 3;
              return eb.or([
                eb(`topic${idx}`, "is", null),
                eb(`fragmentTopic${idx}`, "=", sql.ref(`topic${idx}`)),
              ]);
            });
          }

          return baseJoin;
        })
        .select(["fragmentId", "startBlock", "endBlock"])
        .where("chainId", "=", chainId)
        .execute();

      const intervalsByFragmentId = intervals.reduce(
        (acc, cur) => {
          const { fragmentId, startBlock, endBlock } = cur;
          (acc[fragmentId] ||= []).push([Number(startBlock), Number(endBlock)]);
          return acc;
        },
        {} as Record<string, [number, number][]>,
      );

      const intervalsForEachFragment = fragments.map((f) =>
        intervalUnion(intervalsByFragmentId[f.id] ?? []),
      );
      return intervalIntersectionMany(intervalsForEachFragment);
    });
  };

  insertFactoryChildAddressLogs = async ({
    chainId,
    logs: rpcLogs,
  }: {
    chainId: number;
    logs: RpcLog[];
  }) => {
    return this.db.wrap(
      { method: "insertFactoryChildAddressLogs" },
      async () => {
        if (rpcLogs.length > 0) {
          const logs = rpcLogs.map((rpcLog) => ({
            ...rpcToPostgresLog(rpcLog),
            chainId,
          }));
          await this.db
            .insertInto("logs")
            .values(logs)
            .onConflict((oc) => oc.column("id").doNothing())
            .execute();
        }
      },
    );
  };

  async *getFactoryChildAddresses({
    chainId,
    fromBlock,
    toBlock,
    factory,
    pageSize = 500,
  }: {
    chainId: number;
    fromBlock: bigint;
    toBlock: bigint;
    factory: FactoryLogFilterCriteria | FactoryCallTraceFilterCriteria;
    pageSize?: number;
  }) {
    const { address, eventSelector, childAddressLocation } = factory;
    const selectChildAddressExpression =
      buildFactoryChildAddressSelectExpression({ childAddressLocation });

    const baseQuery = this.db
      .selectFrom("logs")
      .select(["id", selectChildAddressExpression.as("childAddress")])
      .where("chainId", "=", chainId)
      .where("address", "=", address)
      .where("topic0", "=", eventSelector)
      .where("blockNumber", ">=", fromBlock)
      .where("blockNumber", "<=", toBlock)
      .orderBy("id", "asc")
      .limit(pageSize);

    let cursor: string | undefined = undefined;

    while (true) {
      let query = baseQuery;
      if (cursor !== undefined) query = query.where("id", ">", cursor);

      const batch = await this.db.wrap(
        { method: "getFactoryChildAddresses" },
        () => query.execute(),
      );

      if (batch.length > 0) {
        yield batch.map((a) => a.childAddress);
      }

      // If the batch is less than the page size, there are no more pages.
      if (batch.length < pageSize) break;
      // Otherwise, set the cursor to the last block number in the batch.
      cursor = batch[batch.length - 1].id;
    }
  }

  insertFactoryLogFilterInterval = async ({
    chainId,
    factory,
    block: rpcBlock,
    transactions: rpcTransactions,
    transactionReceipts: rpcTransactionReceipts,
    logs: rpcLogs,
    interval,
  }: {
    chainId: number;
    factory: FactoryLogFilterCriteria;
    block: RpcBlock;
    transactions: RpcTransaction[];
    transactionReceipts: RpcTransactionReceipt[];
    logs: RpcLog[];
    interval: { startBlock: bigint; endBlock: bigint };
  }) => {
    return this.db.wrap(
      { method: "insertFactoryLogFilterInterval" },
      async () => {
        await this.db.transaction().execute(async (tx) => {
          await tx
            .insertInto("blocks")
            .values({
              ...rpcToPostgresBlock(rpcBlock),
              chainId,
              checkpoint: this.createBlockCheckpoint(rpcBlock, chainId),
            })
            .onConflict((oc) => oc.column("hash").doNothing())
            .execute();

          if (rpcTransactions.length > 0) {
            const transactions = rpcTransactions.map((transaction) => ({
              ...rpcToPostgresTransaction(transaction),
              chainId,
            }));
            await tx
              .insertInto("transactions")
              .values(transactions)
              .onConflict((oc) => oc.column("hash").doNothing())
              .execute();
          }

          if (rpcTransactionReceipts.length > 0) {
            const transactionReceipts = rpcTransactionReceipts.map(
              (rpcTransactionReceipt) => ({
                ...rpcToPostgresTransactionReceipt(rpcTransactionReceipt),
                chainId,
              }),
            );
            await tx
              .insertInto("transactionReceipts")
              .values(transactionReceipts)
              .onConflict((oc) => oc.column("transactionHash").doNothing())
              .execute();
          }

          if (rpcLogs.length > 0) {
            const logs = rpcLogs.map((rpcLog) => ({
              ...rpcToPostgresLog(rpcLog),
              chainId,
              checkpoint: this.createLogCheckpoint(rpcLog, rpcBlock, chainId),
            }));

            await tx
              .insertInto("logs")
              .values(logs)
              .onConflict((oc) =>
                oc.column("id").doUpdateSet((eb) => ({
                  checkpoint: eb.ref("excluded.checkpoint"),
                })),
              )
              .execute();
          }

          await this._insertFactoryLogFilterInterval({
            tx,
            chainId,
            factoryLogFilters: [factory],
            interval,
          });
        });
      },
    );
  };

  getFactoryLogFilterIntervals = async ({
    chainId,
    factory,
  }: {
    chainId: number;
    factory: FactoryLogFilterCriteria;
  }) => {
    return this.db.wrap(
      { method: "getFactoryLogFilterIntervals" },
      async () => {
        const fragments = buildFactoryLogFragments({ ...factory, chainId });

        await Promise.all(
          fragments.map(async (fragment) => {
            await this.db.transaction().execute(async (tx) => {
              const { id: factoryId } = await tx
                .insertInto("factoryLogFilters")
                .values(fragment)
                .onConflict((oc) => oc.column("id").doUpdateSet(fragment))
                .returningAll()
                .executeTakeFirstOrThrow();

              const existingIntervals = await tx
                .deleteFrom("factoryLogFilterIntervals")
                .where("factoryId", "=", factoryId)
                .returningAll()
                .execute();

              const mergedIntervals = intervalUnion(
                existingIntervals.map((i) => [
                  Number(i.startBlock),
                  Number(i.endBlock),
                ]),
              );

              const mergedIntervalRows = mergedIntervals.map(
                ([startBlock, endBlock]) => ({
                  factoryId,
                  startBlock: BigInt(startBlock),
                  endBlock: BigInt(endBlock),
                }),
              );

              if (mergedIntervalRows.length > 0) {
                await tx
                  .insertInto("factoryLogFilterIntervals")
                  .values(mergedIntervalRows)
                  .execute();
              }
            });
          }),
        );

        const intervals = await this.db
          .with(
            "factoryFilterFragments(fragmentId, fragmentAddress, fragmentEventSelector, fragmentChildAddressLocation, fragmentTopic0, fragmentTopic1, fragmentTopic2, fragmentTopic3, fragmentIncludeTransactionReceipts)",
            () =>
              sql`( values ${sql.join(
                fragments.map(
                  (f) =>
                    sql`( ${sql.val(f.id)}, ${sql.val(f.address)}, ${sql.val(
                      f.eventSelector,
                    )}, ${sql.val(f.childAddressLocation)}, ${sql.val(
                      f.topic0,
                    )}, ${sql.val(f.topic1)}, ${sql.val(f.topic2)}, ${sql.val(
                      f.topic3,
                    )}, ${sql.lit(f.includeTransactionReceipts)} )`,
                ),
              )} )`,
          )
          .selectFrom("factoryLogFilterIntervals")
          .innerJoin("factoryLogFilters", "factoryId", "factoryLogFilters.id")
          .innerJoin("factoryFilterFragments", (join) => {
            let baseJoin = join.on((eb) =>
              eb.and([
                eb("fragmentAddress", "=", sql.ref("address")),
                eb("fragmentEventSelector", "=", sql.ref("eventSelector")),
                eb(
                  "fragmentChildAddressLocation",
                  "=",
                  sql.ref("childAddressLocation"),
                ),
              ]),
            );
            baseJoin = baseJoin.on((eb) =>
              eb(
                "fragmentIncludeTransactionReceipts",
                "<=",
                sql.ref("includeTransactionReceipts"),
              ),
            );
            for (const idx_ of range(0, 4)) {
              baseJoin = baseJoin.on((eb) => {
                const idx = idx_ as 0 | 1 | 2 | 3;
                return eb.or([
                  eb(`topic${idx}`, "is", null),
                  eb(`fragmentTopic${idx}`, "=", sql.ref(`topic${idx}`)),
                ]);
              });
            }

            return baseJoin;
          })
          .select(["fragmentId", "startBlock", "endBlock"])
          .where("chainId", "=", chainId)
          .execute();

        const intervalsByFragmentId = intervals.reduce(
          (acc, cur) => {
            const { fragmentId, startBlock, endBlock } = cur;
            (acc[fragmentId] ||= []).push([
              Number(startBlock),
              Number(endBlock),
            ]);
            return acc;
          },
          {} as Record<string, [number, number][]>,
        );

        const intervalsForEachFragment = fragments.map((f) =>
          intervalUnion(intervalsByFragmentId[f.id] ?? []),
        );
        return intervalIntersectionMany(intervalsForEachFragment);
      },
    );
  };

  insertBlockFilterInterval = async ({
    chainId,
    blockFilter,
    block: rpcBlock,
    interval,
  }: {
    chainId: number;
    blockFilter: BlockFilterCriteria;
    block?: RpcBlock;
    interval: { startBlock: bigint; endBlock: bigint };
  }): Promise<void> => {
    return this.db.wrap({ method: "insertBlockFilterInterval" }, async () => {
      await this.db.transaction().execute(async (tx) => {
        if (rpcBlock !== undefined) {
          await tx
            .insertInto("blocks")
            .values({
              ...rpcToPostgresBlock(rpcBlock),
              chainId,
              checkpoint: this.createBlockCheckpoint(rpcBlock, chainId),
            })
            .onConflict((oc) => oc.column("hash").doNothing())
            .execute();
        }

        await this._insertBlockFilterInterval({
          tx,
          chainId,
          blockFilters: [blockFilter],
          interval,
        });
      });
    });
  };

  getBlockFilterIntervals = async ({
    chainId,
    blockFilter,
  }: {
    chainId: number;
    blockFilter: BlockFilterCriteria;
  }) => {
    return this.db.wrap({ method: "getBlockFilterIntervals" }, async () => {
      const fragment = {
        id: `${chainId}_${blockFilter.interval}_${blockFilter.offset}`,
        chainId,
        interval: blockFilter.interval,
        offset: blockFilter.offset,
      };

      // First, attempt to merge overlapping and adjacent intervals.
      await this.db.transaction().execute(async (tx) => {
        const { id: blockFilterId } = await tx
          .insertInto("blockFilters")
          .values(fragment)
          .onConflict((oc) => oc.column("id").doUpdateSet(fragment))
          .returningAll()
          .executeTakeFirstOrThrow();

        const existingIntervalRows = await tx
          .deleteFrom("blockFilterIntervals")
          .where("blockFilterId", "=", blockFilterId)
          .returningAll()
          .execute();

        const mergedIntervals = intervalUnion(
          existingIntervalRows.map((i) => [
            Number(i.startBlock),
            Number(i.endBlock),
          ]),
        );

        const mergedIntervalRows = mergedIntervals.map(
          ([startBlock, endBlock]) => ({
            blockFilterId,
            startBlock: BigInt(startBlock),
            endBlock: BigInt(endBlock),
          }),
        );

        if (mergedIntervalRows.length > 0) {
          await tx
            .insertInto("blockFilterIntervals")
            .values(mergedIntervalRows)
            .execute();
        }
      });

      const intervals = await this.db
        .selectFrom("blockFilterIntervals")
        .innerJoin("blockFilters", "blockFilterId", "blockFilters.id")
        .select([
          "blockFilterIntervals.startBlock",
          "blockFilterIntervals.endBlock",
        ])
        .where("blockFilterId", "=", fragment.id)
        .execute();

      return intervals.map(
        ({ startBlock, endBlock }) =>
          [Number(startBlock), Number(endBlock)] as [number, number],
      );
    });
  };

  getBlock = async ({
    chainId,
    blockNumber,
  }: {
    chainId: number;
    blockNumber: number;
  }): Promise<boolean> => {
    const hasBlock = await this.db
      .selectFrom("blocks")
      .select("hash")
      .where("number", "=", BigInt(blockNumber))
      .where("chainId", "=", chainId)
      .executeTakeFirst();

    return hasBlock !== undefined;
  };

  insertTraceFilterInterval = async ({
    chainId,
    traceFilter,
    block: rpcBlock,
    transactions: rpcTransactions,
    transactionReceipts: rpcTransactionReceipts,
    traces: rpcTraces,
    interval,
  }: {
    chainId: number;
    traceFilter: CallTraceFilterCriteria;
    block: RpcBlock;
    transactions: RpcTransaction[];
    transactionReceipts: RpcTransactionReceipt[];
    traces: SyncCallTrace[];
    interval: { startBlock: bigint; endBlock: bigint };
  }) => {
    return this.db.wrap({ method: "insertTraceFilterInterval" }, async () => {
      await this.db.transaction().execute(async (tx) => {
        await tx
          .insertInto("blocks")
          .values({
            ...rpcToPostgresBlock(rpcBlock),
            chainId,
            checkpoint: this.createBlockCheckpoint(rpcBlock, chainId),
          })
          .onConflict((oc) => oc.column("hash").doNothing())
          .execute();

        if (rpcTransactions.length > 0) {
          const transactions = rpcTransactions.map((transaction) => ({
            ...rpcToPostgresTransaction(transaction),
            chainId,
          }));
          await tx
            .insertInto("transactions")
            .values(transactions)
            .onConflict((oc) => oc.column("hash").doNothing())
            .execute();
        }

        if (rpcTransactionReceipts.length > 0) {
          const transactionReceipts = rpcTransactionReceipts.map(
            (rpcTransactionReceipt) => ({
              ...rpcToPostgresTransactionReceipt(rpcTransactionReceipt),
              chainId,
            }),
          );
          await tx
            .insertInto("transactionReceipts")
            .values(transactionReceipts)
            .onConflict((oc) => oc.column("transactionHash").doNothing())
            .execute();
        }

        // Delete existing traces with the same `transactionHash`. Then, calculate "callTraces.checkpoint"
        // based on the ordering of "callTraces.traceAddress" and add all traces to "callTraces" table.
        const traceByTransactionHash: {
          [transactionHash: Hex]: SyncCallTrace[];
        } = {};
        for (const trace of rpcTraces) {
          if (traceByTransactionHash[trace.transactionHash] === undefined) {
            traceByTransactionHash[trace.transactionHash] = [];
          }
          traceByTransactionHash[trace.transactionHash].push(trace);
        }

        for (const transactionHash of Object.keys(traceByTransactionHash)) {
          const traces = await tx
            .deleteFrom("callTraces")
            .returningAll()
            .where("transactionHash", "=", transactionHash as Hex)
            .where("chainId", "=", chainId)
            .execute();

          (traces as Omit<InsertableCallTrace, "checkpoint">[]).push(
            ...traceByTransactionHash[transactionHash as Hex].map((trace) => ({
              ...rpcToPostgresTrace(trace),
              chainId,
            })),
          );

          // Use lexographical sort of stringified `traceAddress`.
          traces.sort((a, b) => {
            return a.traceAddress < b.traceAddress ? -1 : 1;
          });

          for (let i = 0; i < traces.length; i++) {
            const trace = traces[i];
            const checkpoint = encodeCheckpoint({
              blockTimestamp: hexToNumber(rpcBlock.timestamp),
              chainId: BigInt(chainId),
              blockNumber: trace.blockNumber,
              transactionIndex: BigInt(trace.transactionPosition),
              eventType: EVENT_TYPES.callTraces,
              eventIndex: BigInt(i),
            });

            trace.checkpoint = checkpoint;
          }

          await tx
            .insertInto("callTraces")
            .values(traces)
            .onConflict((oc) => oc.column("id").doNothing())
            .execute();
        }

        await this._insertTraceFilterInterval({
          tx,
          chainId,
          traceFilters: [traceFilter],
          interval,
        });
      });
    });
  };

  getTraceFilterIntervals = async ({
    traceFilter,
    chainId,
  }: {
    chainId: number;
    traceFilter: CallTraceFilterCriteria;
  }) => {
    return this.db.wrap({ method: "getTraceFilterIntervals" }, async () => {
      const fragments = buildTraceFragments({ ...traceFilter, chainId });

      // First, attempt to merge overlapping and adjacent intervals.
      await Promise.all(
        fragments.map(async (fragment) => {
          return await this.db.transaction().execute(async (tx) => {
            const { id: traceFilterId } = await tx
              .insertInto("traceFilters")
              .values(fragment)
              .onConflict((oc) => oc.column("id").doUpdateSet(fragment))
              .returningAll()
              .executeTakeFirstOrThrow();

            const existingIntervalRows = await tx
              .deleteFrom("traceFilterIntervals")
              .where("traceFilterId", "=", traceFilterId)
              .returningAll()
              .execute();

            const mergedIntervals = intervalUnion(
              existingIntervalRows.map((i) => [
                Number(i.startBlock),
                Number(i.endBlock),
              ]),
            );

            const mergedIntervalRows = mergedIntervals.map(
              ([startBlock, endBlock]) => ({
                traceFilterId,
                startBlock: BigInt(startBlock),
                endBlock: BigInt(endBlock),
              }),
            );

            if (mergedIntervalRows.length > 0) {
              await tx
                .insertInto("traceFilterIntervals")
                .values(mergedIntervalRows)
                .execute();
            }
          });
        }),
      );

      const intervals = await this.db
        .with(
          "traceFilterFragments(fragmentId, fragmentFromAddress, fragmentToAddress)",
          () =>
            sql`( values ${sql.join(
              fragments.map(
                (f) =>
                  sql`( ${sql.val(f.id)}, ${sql.val(f.fromAddress)}, ${sql.val(
                    f.toAddress,
                  )} )`,
              ),
            )} )`,
        )
        .selectFrom("traceFilterIntervals")
        .innerJoin("traceFilters", "traceFilterId", "traceFilters.id")
        .innerJoin("traceFilterFragments", (join) => {
          return join.on((eb) =>
            eb.and([
              eb.or([
                eb("fromAddress", "is", null),
                eb("fragmentFromAddress", "=", sql.ref("fromAddress")),
              ]),
              eb.or([
                eb("toAddress", "is", null),
                eb("fragmentToAddress", "=", sql.ref("toAddress")),
              ]),
            ]),
          );
        })
        .select(["fragmentId", "startBlock", "endBlock"])
        .where("chainId", "=", chainId)
        .execute();

      const intervalsByFragmentId = intervals.reduce(
        (acc, cur) => {
          const { fragmentId, startBlock, endBlock } = cur;
          (acc[fragmentId] ||= []).push([Number(startBlock), Number(endBlock)]);
          return acc;
        },
        {} as Record<string, [number, number][]>,
      );

      const intervalsForEachFragment = fragments.map((f) =>
        intervalUnion(intervalsByFragmentId[f.id] ?? []),
      );
      return intervalIntersectionMany(intervalsForEachFragment);
    });
  };

  insertFactoryTraceFilterInterval = async ({
    chainId,
    factory,
    block: rpcBlock,
    transactions: rpcTransactions,
    transactionReceipts: rpcTransactionReceipts,
    traces: rpcTraces,
    interval,
  }: {
    chainId: number;
    factory: FactoryCallTraceFilterCriteria;
    block: RpcBlock;
    transactions: RpcTransaction[];
    transactionReceipts: RpcTransactionReceipt[];
    traces: SyncCallTrace[];
    interval: { startBlock: bigint; endBlock: bigint };
  }) => {
    return this.db.wrap(
      { method: "insertFactoryTraceFilterInterval" },
      async () => {
        await this.db.transaction().execute(async (tx) => {
          await tx
            .insertInto("blocks")
            .values({
              ...rpcToPostgresBlock(rpcBlock),
              chainId,
              checkpoint: this.createBlockCheckpoint(rpcBlock, chainId),
            })
            .onConflict((oc) => oc.column("hash").doNothing())
            .execute();

          if (rpcTransactions.length > 0) {
            const transactions = rpcTransactions.map((rpcTransaction) => ({
              ...rpcToPostgresTransaction(rpcTransaction),
              chainId,
            }));
            await tx
              .insertInto("transactions")
              .values(transactions)
              .onConflict((oc) => oc.column("hash").doNothing())
              .execute();
          }

          if (rpcTransactionReceipts.length > 0) {
            const transactionReceipts = rpcTransactionReceipts.map(
              (rpcTransactionReceipt) => ({
                ...rpcToPostgresTransactionReceipt(rpcTransactionReceipt),
                chainId,
              }),
            );
            await tx
              .insertInto("transactionReceipts")
              .values(transactionReceipts)
              .onConflict((oc) => oc.column("transactionHash").doNothing())
              .execute();
          }

          // Delete existing traces with the same `transactionHash`. Then, calculate "callTraces.checkpoint"
          // based on the ordering of "callTraces.traceAddress" and add all traces to "callTraces" table.
          const traceByTransactionHash: {
            [transactionHash: Hex]: SyncCallTrace[];
          } = {};
          for (const trace of rpcTraces) {
            if (traceByTransactionHash[trace.transactionHash] === undefined) {
              traceByTransactionHash[trace.transactionHash] = [];
            }
            traceByTransactionHash[trace.transactionHash].push(trace);
          }

          for (const transactionHash of Object.keys(traceByTransactionHash)) {
            const traces = await tx
              .deleteFrom("callTraces")
              .returningAll()
              .where("transactionHash", "=", transactionHash as Hex)
              .where("chainId", "=", chainId)
              .execute();

            (traces as Omit<InsertableCallTrace, "checkpoint">[]).push(
              ...traceByTransactionHash[transactionHash as Hex].map(
                (trace) => ({
                  ...rpcToPostgresTrace(trace),
                  chainId,
                }),
              ),
            );

            // Use lexographical sort of stringified `traceAddress`.
            traces.sort((a, b) => {
              return a.traceAddress < b.traceAddress ? -1 : 1;
            });

            for (let i = 0; i < traces.length; i++) {
              const trace = traces[i];
              const checkpoint = encodeCheckpoint({
                blockTimestamp: hexToNumber(rpcBlock.timestamp),
                chainId: BigInt(chainId),
                blockNumber: trace.blockNumber,
                transactionIndex: BigInt(trace.transactionPosition),
                eventType: EVENT_TYPES.callTraces,
                eventIndex: BigInt(i),
              });

              trace.checkpoint = checkpoint;
            }

            await tx
              .insertInto("callTraces")
              .values(traces)
              .onConflict((oc) => oc.column("id").doNothing())
              .execute();
          }

          await this._insertFactoryTraceFilterInterval({
            tx,
            chainId,
            factoryTraceFilters: [factory],
            interval,
          });
        });
      },
    );
  };

  getFactoryTraceFilterIntervals = async ({
    chainId,
    factory,
  }: {
    chainId: number;
    factory: FactoryCallTraceFilterCriteria;
  }) => {
    return this.db.wrap(
      { method: "getFactoryLogFilterIntervals" },
      async () => {
        const fragments = buildFactoryTraceFragments({ ...factory, chainId });

        await Promise.all(
          fragments.map(async (fragment) => {
            return await this.db.transaction().execute(async (tx) => {
              const { id: factoryId } = await tx
                .insertInto("factoryTraceFilters")
                .values(fragment)
                .onConflict((oc) => oc.column("id").doUpdateSet(fragment))
                .returningAll()
                .executeTakeFirstOrThrow();

              const existingIntervals = await tx
                .deleteFrom("factoryTraceFilterIntervals")
                .where("factoryId", "=", factoryId)
                .returningAll()
                .execute();

              const mergedIntervals = intervalUnion(
                existingIntervals.map((i) => [
                  Number(i.startBlock),
                  Number(i.endBlock),
                ]),
              );

              const mergedIntervalRows = mergedIntervals.map(
                ([startBlock, endBlock]) => ({
                  factoryId,
                  startBlock: BigInt(startBlock),
                  endBlock: BigInt(endBlock),
                }),
              );

              if (mergedIntervalRows.length > 0) {
                await tx
                  .insertInto("factoryTraceFilterIntervals")
                  .values(mergedIntervalRows)
                  .execute();
              }
            });
          }),
        );

        const intervals = await this.db
          .with(
            "factoryFilterFragments(fragmentId, fragmentAddress, fragmentEventSelector, fragmentChildAddressLocation, fragmentFromAddress)",
            () =>
              sql`( values ${sql.join(
                fragments.map(
                  (f) =>
                    sql`( ${sql.val(f.id)}, ${sql.val(f.address)}, ${sql.val(
                      f.eventSelector,
                    )}, ${sql.val(f.childAddressLocation)}, ${sql.val(
                      f.fromAddress,
                    )} )`,
                ),
              )} )`,
          )
          .selectFrom("factoryTraceFilterIntervals")
          .innerJoin(
            "factoryTraceFilters",
            "factoryId",
            "factoryTraceFilters.id",
          )
          .innerJoin("factoryFilterFragments", (join) =>
            join.on((eb) =>
              eb.and([
                eb("fragmentAddress", "=", sql.ref("address")),
                eb("fragmentEventSelector", "=", sql.ref("eventSelector")),
                eb(
                  "fragmentChildAddressLocation",
                  "=",
                  sql.ref("childAddressLocation"),
                ),
                eb.or([
                  eb("fromAddress", "is", null),
                  eb("fragmentFromAddress", "=", sql.ref("fromAddress")),
                ]),
              ]),
            ),
          )
          .select(["fragmentId", "startBlock", "endBlock"])
          .where("chainId", "=", chainId)
          .execute();

        const intervalsByFragmentId = intervals.reduce(
          (acc, cur) => {
            const { fragmentId, startBlock, endBlock } = cur;
            (acc[fragmentId] ||= []).push([
              Number(startBlock),
              Number(endBlock),
            ]);
            return acc;
          },
          {} as Record<string, [number, number][]>,
        );

        const intervalsForEachFragment = fragments.map((f) =>
          intervalUnion(intervalsByFragmentId[f.id] ?? []),
        );
        return intervalIntersectionMany(intervalsForEachFragment);
      },
    );
  };

  private createLogCheckpoint = (
    rpcLog: RpcLog,
    block: RpcBlock,
    chainId: number,
  ) => {
    if (block.number === null) {
      throw new Error("Number is missing from RPC block");
    }
    if (rpcLog.transactionIndex === null) {
      throw new Error("Transaction index is missing from RPC log");
    }
    if (rpcLog.logIndex === null) {
      throw new Error("Log index is missing from RPC log");
    }
    return encodeCheckpoint({
      blockTimestamp: Number(BigInt(block.timestamp)),
      chainId: BigInt(chainId),
      blockNumber: hexToBigInt(block.number),
      transactionIndex: hexToBigInt(rpcLog.transactionIndex),
      eventType: EVENT_TYPES.logs,
      eventIndex: hexToBigInt(rpcLog.logIndex),
    });
  };

  private createBlockCheckpoint = (block: RpcBlock, chainId: number) => {
    if (block.number === null) {
      throw new Error("Number is missing from RPC block");
    }

    return encodeCheckpoint({
      blockTimestamp: hexToNumber(block.timestamp),
      chainId: BigInt(chainId),
      blockNumber: hexToBigInt(block.number),
      transactionIndex: maxCheckpoint.transactionIndex,
      eventType: EVENT_TYPES.blocks,
      eventIndex: zeroCheckpoint.eventIndex,
    });
  };

  insertRealtimeBlock = async ({
    chainId,
    block: rpcBlock,
    transactions: rpcTransactions,
    transactionReceipts: rpcTransactionReceipts,
    logs: rpcLogs,
    traces: rpcTraces,
  }: {
    chainId: number;
    block: RpcBlock;
    transactions: RpcTransaction[];
    transactionReceipts: RpcTransactionReceipt[];
    logs: RpcLog[];
    traces: SyncCallTrace[];
  }) => {
    return this.db.wrap({ method: "insertRealtimeBlock" }, async () => {
      await this.db.transaction().execute(async (tx) => {
        await tx
          .insertInto("blocks")
          .values({
            ...rpcToPostgresBlock(rpcBlock),
            chainId,
            checkpoint: this.createBlockCheckpoint(rpcBlock, chainId),
          })
          .onConflict((oc) => oc.column("hash").doNothing())
          .execute();

        if (rpcTransactions.length > 0) {
          const transactions = rpcTransactions.map((transaction) => ({
            ...rpcToPostgresTransaction(transaction),
            chainId,
          }));
          await tx
            .insertInto("transactions")
            .values(transactions)
            .onConflict((oc) =>
              oc.column("hash").doUpdateSet((eb) => ({
                blockHash: eb.ref("excluded.blockHash"),
                blockNumber: eb.ref("excluded.blockNumber"),
                transactionIndex: eb.ref("excluded.transactionIndex"),
              })),
            )
            .execute();
        }

        if (rpcTransactionReceipts.length > 0) {
          const transactionReceipts = rpcTransactionReceipts.map(
            (rpcTransactionReceipt) => ({
              ...rpcToPostgresTransactionReceipt(rpcTransactionReceipt),
              chainId,
            }),
          );
          await tx
            .insertInto("transactionReceipts")
            .values(transactionReceipts)
            .onConflict((oc) =>
              oc.column("transactionHash").doUpdateSet((eb) => ({
                blockHash: eb.ref("excluded.blockHash"),
                blockNumber: eb.ref("excluded.blockNumber"),
                contractAddress: eb.ref("excluded.contractAddress"),
                cumulativeGasUsed: eb.ref("excluded.cumulativeGasUsed"),
                effectiveGasPrice: eb.ref("excluded.effectiveGasPrice"),
                gasUsed: eb.ref("excluded.gasUsed"),
                logs: eb.ref("excluded.logs"),
                logsBloom: eb.ref("excluded.logsBloom"),
                transactionIndex: eb.ref("excluded.transactionIndex"),
              })),
            )
            .execute();
        }

        if (rpcLogs.length > 0) {
          const logs = rpcLogs.map((rpcLog) => ({
            ...rpcToPostgresLog(rpcLog),
            chainId,
            checkpoint: this.createLogCheckpoint(rpcLog, rpcBlock, chainId),
          }));
          await tx
            .insertInto("logs")
            .values(logs)
            .onConflict((oc) =>
              oc.column("id").doUpdateSet((eb) => ({
                checkpoint: eb.ref("excluded.checkpoint"),
              })),
            )
            .execute();
        }

        if (rpcTraces.length > 0) {
          const traces = rpcTraces
            .map((trace, i) => ({
              ...rpcToPostgresTrace(trace),
              chainId,
              checkpoint: encodeCheckpoint({
                blockTimestamp: hexToNumber(rpcBlock.timestamp),
                chainId: BigInt(chainId),
                blockNumber: hexToBigInt(trace.blockNumber),
                transactionIndex: BigInt(trace.transactionPosition),
                eventType: EVENT_TYPES.callTraces,
                eventIndex: BigInt(i),
              }),
            }))
            .sort((a, b) => {
              if (a.transactionHash < b.transactionHash) return -1;
              if (a.transactionHash > b.transactionHash) return 1;
              return a.traceAddress < b.traceAddress ? -1 : 1;
            });

          await tx
            .insertInto("callTraces")
            .values(traces)
            .onConflict((oc) => oc.column("id").doNothing())
            .execute();
        }
      });
    });
  };

  insertRealtimeInterval = async ({
    chainId,
    logFilters,
    factoryLogFilters,
    traceFilters,
    factoryTraceFilters,
    blockFilters,
    interval,
  }: {
    chainId: number;
    logFilters: LogFilterCriteria[];
    factoryLogFilters: FactoryLogFilterCriteria[];
    traceFilters: CallTraceFilterCriteria[];
    factoryTraceFilters: FactoryCallTraceFilterCriteria[];
    blockFilters: BlockFilterCriteria[];
    interval: { startBlock: bigint; endBlock: bigint };
  }) => {
    return this.db.wrap({ method: "insertRealtimeInterval" }, async () => {
      await this.db.transaction().execute(async (tx) => {
        await this._insertLogFilterInterval({
          tx,
          chainId,
          logFilters: [
            ...logFilters,
            ...factoryLogFilters.map((f) => ({
              address: f.address,
              topics: [f.eventSelector],
              includeTransactionReceipts: f.includeTransactionReceipts,
            })),
            ...factoryTraceFilters.map((f) => ({
              address: f.address,
              topics: [f.eventSelector],
              includeTransactionReceipts: f.includeTransactionReceipts,
            })),
          ],
          interval,
        });

        await this._insertFactoryLogFilterInterval({
          tx,
          chainId,
          factoryLogFilters,
          interval,
        });

        await this._insertBlockFilterInterval({
          tx,
          chainId,
          blockFilters,
          interval,
        });

        await this._insertTraceFilterInterval({
          tx,
          chainId,
          traceFilters,
          interval,
        });

        await this._insertFactoryTraceFilterInterval({
          tx,
          chainId,
          factoryTraceFilters,
          interval,
        });
      });
    });
  };

  deleteRealtimeData = async ({
    chainId,
    fromBlock,
  }: {
    chainId: number;
    fromBlock: bigint;
  }) => {
    return this.db.wrap({ method: "deleteRealtimeData" }, async () => {
      await this.db.transaction().execute(async (tx) => {
        await tx
          .deleteFrom("logs")
          .where("chainId", "=", chainId)
          .where("blockNumber", ">", fromBlock)
          .execute();
        await tx
          .deleteFrom("blocks")
          .where("chainId", "=", chainId)
          .where("number", ">", fromBlock)
          .execute();
        await tx
          .deleteFrom("rpcRequestResults")
          .where("chainId", "=", chainId)
          .where("blockNumber", ">", fromBlock)
          .execute();
        await tx
          .deleteFrom("callTraces")
          .where("chainId", "=", chainId)
          .where("blockNumber", ">", fromBlock)
          .execute();
      });
    });
  };

  /** SYNC HELPER METHODS */

  private _insertLogFilterInterval = async ({
    tx,
    chainId,
    logFilters,
    interval: { startBlock, endBlock },
  }: {
    tx: KyselyTransaction<SyncStoreTables>;
    chainId: number;
    logFilters: LogFilterCriteria[];
    interval: { startBlock: bigint; endBlock: bigint };
  }) => {
    const logFilterFragments = logFilters.flatMap((logFilter) =>
      buildLogFilterFragments({ ...logFilter, chainId }),
    );

    await Promise.all(
      logFilterFragments.map(async (logFilterFragment) => {
        const { id: logFilterId } = await tx
          .insertInto("logFilters")
          .values(logFilterFragment)
          .onConflict((oc) => oc.column("id").doUpdateSet(logFilterFragment))
          .returningAll()
          .executeTakeFirstOrThrow();

        await tx
          .insertInto("logFilterIntervals")
          .values({ logFilterId, startBlock, endBlock })
          .execute();
      }),
    );
  };

  private _insertFactoryLogFilterInterval = async ({
    tx,
    chainId,
    factoryLogFilters,
    interval: { startBlock, endBlock },
  }: {
    tx: KyselyTransaction<SyncStoreTables>;
    chainId: number;
    factoryLogFilters: FactoryLogFilterCriteria[];
    interval: { startBlock: bigint; endBlock: bigint };
  }) => {
    const factoryFragments = factoryLogFilters.flatMap((factory) =>
      buildFactoryLogFragments({ ...factory, chainId }),
    );

    await Promise.all(
      factoryFragments.map(async (fragment) => {
        const { id: factoryId } = await tx
          .insertInto("factoryLogFilters")
          .values(fragment)
          .onConflict((oc) => oc.column("id").doUpdateSet(fragment))
          .returningAll()
          .executeTakeFirstOrThrow();

        await tx
          .insertInto("factoryLogFilterIntervals")
          .values({ factoryId, startBlock, endBlock })
          .execute();
      }),
    );
  };

  private _insertBlockFilterInterval = async ({
    tx,
    chainId,
    blockFilters,
    interval: { startBlock, endBlock },
  }: {
    tx: KyselyTransaction<SyncStoreTables>;
    chainId: number;
    blockFilters: BlockFilterCriteria[];
    interval: { startBlock: bigint; endBlock: bigint };
  }) => {
    const blockFilterFragments = blockFilters.map((blockFilter) => {
      return {
        id: `${chainId}_${blockFilter.interval}_${blockFilter.offset}`,
        chainId,
        interval: blockFilter.interval,
        offset: blockFilter.offset,
      };
    });

    await Promise.all(
      blockFilterFragments.map(async (blockFilterFragment) => {
        const { id: blockFilterId } = await tx
          .insertInto("blockFilters")
          .values(blockFilterFragment)
          .onConflict((oc) => oc.column("id").doUpdateSet(blockFilterFragment))
          .returningAll()
          .executeTakeFirstOrThrow();

        await tx
          .insertInto("blockFilterIntervals")
          .values({ blockFilterId, startBlock, endBlock })
          .execute();
      }),
    );
  };

  private _insertTraceFilterInterval = async ({
    tx,
    chainId,
    traceFilters,
    interval: { startBlock, endBlock },
  }: {
    tx: KyselyTransaction<SyncStoreTables>;
    chainId: number;
    traceFilters: CallTraceFilterCriteria[];
    interval: { startBlock: bigint; endBlock: bigint };
  }) => {
    const traceFilterFragments = traceFilters.flatMap((traceFilter) =>
      buildTraceFragments({ ...traceFilter, chainId }),
    );

    await Promise.all(
      traceFilterFragments.map(async (traceFilterFragment) => {
        const { id: traceFilterId } = await tx
          .insertInto("traceFilters")
          .values(traceFilterFragment)
          .onConflict((oc) => oc.column("id").doUpdateSet(traceFilterFragment))
          .returningAll()
          .executeTakeFirstOrThrow();

        await tx
          .insertInto("traceFilterIntervals")
          .values({ traceFilterId, startBlock, endBlock })
          .execute();
      }),
    );
  };

  private _insertFactoryTraceFilterInterval = async ({
    tx,
    chainId,
    factoryTraceFilters,
    interval: { startBlock, endBlock },
  }: {
    tx: KyselyTransaction<SyncStoreTables>;
    chainId: number;
    factoryTraceFilters: FactoryCallTraceFilterCriteria[];
    interval: { startBlock: bigint; endBlock: bigint };
  }) => {
    const factoryFragments = factoryTraceFilters.flatMap((factory) =>
      buildFactoryTraceFragments({ ...factory, chainId }),
    );

    await Promise.all(
      factoryFragments.map(async (fragment) => {
        const { id: factoryId } = await tx
          .insertInto("factoryTraceFilters")
          .values(fragment)
          .onConflict((oc) => oc.column("id").doUpdateSet(fragment))
          .returningAll()
          .executeTakeFirstOrThrow();

        await tx
          .insertInto("factoryTraceFilterIntervals")
          .values({
            factoryId,
            startBlock,
            endBlock,
          })
          .execute();
      }),
    );
  };

  insertRpcRequestResult = async ({
    request,
    blockNumber,
    chainId,
    result,
  }: {
    request: string;
    blockNumber: bigint;
    chainId: number;
    result: string;
  }) => {
    return this.db.wrap({ method: "insertRpcRequestResult" }, async () => {
      await this.db
        .insertInto("rpcRequestResults")
        .values({ request, blockNumber, chainId, result })
        .onConflict((oc) =>
          oc.constraint("rpcRequestResultPrimaryKey").doUpdateSet({ result }),
        )
        .execute();
    });
  };

  getRpcRequestResult = async ({
    request,
    blockNumber,
    chainId,
  }: {
    request: string;
    blockNumber: bigint;
    chainId: number;
  }) => {
    return this.db.wrap({ method: "getRpcRequestResult" }, async () => {
      const contractReadResult = await this.db
        .selectFrom("rpcRequestResults")
        .selectAll()
        .where("request", "=", request)
        .where("blockNumber", "=", blockNumber)
        .where("chainId", "=", chainId)
        .executeTakeFirst();

      return contractReadResult ?? null;
    });
  };

  async *getEvents({
    sources,
    fromCheckpoint,
    toCheckpoint,
    limit,
  }: {
    sources: EventSource[];
    fromCheckpoint: Checkpoint;
    toCheckpoint: Checkpoint;
    limit: number;
  }) {
    let cursor = encodeCheckpoint(fromCheckpoint);
    const encodedToCheckpoint = encodeCheckpoint(toCheckpoint);

    const sourcesById = sources.reduce<{
      [sourceId: string]: (typeof sources)[number];
    }>((acc, cur) => {
      acc[cur.id] = cur;
      return acc;
    }, {});

    // We can assume that source won't be empty.
    const logSources = sources.filter(
      (s): s is LogSource | FactoryLogSource =>
        sourceIsLog(s) || sourceIsFactoryLog(s),
    );
    const callTraceSources = sources.filter(
      (s): s is CallTraceSource | FactoryCallTraceSource =>
        sourceIsCallTrace(s) || sourceIsFactoryCallTrace(s),
    );
    const blockSources = sources.filter(sourceIsBlock);

    const shouldJoinLogs = logSources.length !== 0;
    const shouldJoinTransactions =
      logSources.length !== 0 || callTraceSources.length !== 0;
    const shouldJoinTraces = callTraceSources.length !== 0;
    const shouldJoinReceipts =
      logSources.some((source) => source.criteria.includeTransactionReceipts) ||
      callTraceSources.some(
        (source) => source.criteria.includeTransactionReceipts,
      );

    while (true) {
      const events = await this.db.wrap({ method: "getEvents" }, async () => {
        // Get full log objects, including the eventSelector clause.
        const requestedLogs = await this.db
          .with(
            "log_sources(source_id)",
            () =>
              sql`( values ${
                logSources.length === 0
                  ? sql`( null )`
                  : sql.join(
                      logSources.map(
                        (source) => sql`( ${sql.val(source.id)} )`,
                      ),
                    )
              } )`,
          )
          .with(
            "block_sources(source_id)",
            () =>
              sql`( values ${
                blockSources.length === 0
                  ? sql`( null )`
                  : sql.join(
                      blockSources.map(
                        (source) => sql`( ${sql.val(source.id)} )`,
                      ),
                    )
              } )`,
          )
          .with(
            "call_trace_sources(source_id)",
            () =>
              sql`( values ${
                callTraceSources.length === 0
                  ? sql`( null )`
                  : sql.join(
                      callTraceSources.map(
                        (source) => sql`( ${sql.val(source.id)} )`,
                      ),
                    )
              } )`,
          )
          .with("log_checkpoints", (db) =>
            db
              .selectFrom("logs")
              .where("logs.checkpoint", ">", cursor)
              .where("logs.checkpoint", "<=", encodedToCheckpoint)
              .orderBy("logs.checkpoint", "asc")
              .limit(limit)
              .select("logs.checkpoint"),
          )
          .with("block_checkpoints", (db) =>
            db
              .selectFrom("blocks")
              .where("blocks.checkpoint", ">", cursor)
              .where("blocks.checkpoint", "<=", encodedToCheckpoint)
              .orderBy("blocks.checkpoint", "asc")
              .limit(limit)
              .select("blocks.checkpoint"),
          )
          .with("call_trace_checkpoints", (db) =>
            db
              .selectFrom("callTraces")
              .where("callTraces.checkpoint", ">", cursor)
              .where("callTraces.checkpoint", "<=", encodedToCheckpoint)
              .orderBy("callTraces.checkpoint", "asc")
              .limit(limit)
              .select("callTraces.checkpoint"),
          )
          .with("max_checkpoint", (db) =>
            db
              .selectFrom(
                db
                  .selectFrom("log_checkpoints")
                  .select("checkpoint")
                  .unionAll(
                    db.selectFrom("block_checkpoints").select("checkpoint"),
                  )
                  .unionAll(
                    db
                      .selectFrom("call_trace_checkpoints")
                      .select("checkpoint"),
                  )
                  .as("all_checkpoints"),
              )
              .select(
                sql`COALESCE(max(checkpoint), ${encodedToCheckpoint})`.as(
                  "max_checkpoint",
                ),
              ),
          )
          .with("events", (db) =>
            db
              .selectFrom("logs")
              .innerJoin("log_sources", (join) => join.onTrue())
              .where((eb) => {
                const logFilterCmprs = sources
                  .filter(sourceIsLog)
                  .map((logFilter) => {
                    const exprs = this.buildLogFilterCmprs({
                      eb,
                      logFilter,
                    });
                    exprs.push(eb("source_id", "=", logFilter.id));
                    return eb.and(exprs);
                  });

                const factoryCmprs = sources
                  .filter(sourceIsFactoryLog)
                  .map((factory) => {
                    const exprs = this.buildFactoryLogFilterCmprs({
                      eb,
                      factory,
                    });
                    exprs.push(eb("source_id", "=", factory.id));
                    return eb.and(exprs);
                  });

                return eb.or([...logFilterCmprs, ...factoryCmprs]);
              })
              .select([
                "source_id",
                "checkpoint",
                "blockHash",
                "transactionHash",

                "logs.id as log_id",
                sql`null`.as("callTrace_id"),
              ])
              .unionAll(
                // @ts-ignore
                db
                  .selectFrom("blocks")
                  .innerJoin("block_sources", (join) => join.onTrue())
                  .where((eb) => {
                    const exprs = [];
                    for (const blockSource of blockSources) {
                      exprs.push(
                        eb.and([
                          eb("chainId", "=", blockSource.chainId),
                          eb("number", ">=", BigInt(blockSource.startBlock)),
                          ...(blockSource.endBlock !== undefined
                            ? [eb("number", "<=", BigInt(blockSource.endBlock))]
                            : []),
                          sql`(number - ${sql.val(
                            blockSource.criteria.offset,
                          )}) % ${sql.val(blockSource.criteria.interval)} = 0`,
                          eb("source_id", "=", blockSource.id),
                        ]),
                      );
                    }
                    return eb.or(exprs);
                  })
                  .select([
                    "block_sources.source_id",
                    "checkpoint",
                    "hash as blockHash",
                    sql`null`.as("transactionHash"),

                    sql`null`.as("log_id"),
                    sql`null`.as("callTrace_id"),
                  ]),
              )
              .unionAll(
                // @ts-ignore
                db
                  .selectFrom("callTraces")
                  .innerJoin("call_trace_sources", (join) => join.onTrue())
                  .where((eb) => {
                    const traceFilterCmprs = sources
                      .filter(sourceIsCallTrace)
                      .map((callTraceSource) => {
                        const exprs = this.buildTraceFilterCmprs({
                          eb,
                          callTraceSource,
                        });
                        exprs.push(eb("source_id", "=", callTraceSource.id));
                        return eb.and(exprs);
                      });
                    const factoryTraceFilterCmprs = sources
                      .filter(sourceIsFactoryCallTrace)
                      .map((factory) => {
                        const exprs = this.buildFactoryTraceFilterCmprs({
                          eb,
                          factory,
                        });
                        exprs.push(eb("source_id", "=", factory.id));
                        return eb.and(exprs);
                      });

                    return eb.or([
                      ...traceFilterCmprs,
                      ...factoryTraceFilterCmprs,
                    ]);
                  })

                  .select([
                    "source_id",
                    "checkpoint",
                    "blockHash",
                    "transactionHash",

                    sql`null`.as("log_id"),
                    "callTraces.id as callTrace_id",
                  ]),
              ),
          )
          .selectFrom("events")
          .innerJoin("blocks", "blocks.hash", "events.blockHash")
          .select([
            "events.source_id",
            "events.checkpoint",

            "blocks.baseFeePerGas as block_baseFeePerGas",
            "blocks.difficulty as block_difficulty",
            "blocks.extraData as block_extraData",
            "blocks.gasLimit as block_gasLimit",
            "blocks.gasUsed as block_gasUsed",
            "blocks.hash as block_hash",
            "blocks.logsBloom as block_logsBloom",
            "blocks.miner as block_miner",
            "blocks.mixHash as block_mixHash",
            "blocks.nonce as block_nonce",
            "blocks.number as block_number",
            "blocks.parentHash as block_parentHash",
            "blocks.receiptsRoot as block_receiptsRoot",
            "blocks.sha3Uncles as block_sha3Uncles",
            "blocks.size as block_size",
            "blocks.stateRoot as block_stateRoot",
            "blocks.timestamp as block_timestamp",
            "blocks.totalDifficulty as block_totalDifficulty",
            "blocks.transactionsRoot as block_transactionsRoot",
          ])
          .$if(shouldJoinLogs, (qb) =>
            qb
              .leftJoin("logs", "logs.id", "events.log_id")
              .select([
                "logs.address as log_address",
                "logs.blockHash as log_blockHash",
                "logs.blockNumber as log_blockNumber",
                "logs.chainId as log_chainId",
                "logs.data as log_data",
                "logs.id as log_id",
                "logs.logIndex as log_logIndex",
                "logs.topic0 as log_topic0",
                "logs.topic1 as log_topic1",
                "logs.topic2 as log_topic2",
                "logs.topic3 as log_topic3",
                "logs.transactionHash as log_transactionHash",
                "logs.transactionIndex as log_transactionIndex",
              ]),
          )
          .$if(shouldJoinTransactions, (qb) =>
            qb
              .leftJoin(
                "transactions",
                "transactions.hash",
                "events.transactionHash",
              )
              .select([
                "transactions.accessList as tx_accessList",
                "transactions.blockHash as tx_blockHash",
                "transactions.blockNumber as tx_blockNumber",
                "transactions.from as tx_from",
                "transactions.gas as tx_gas",
                "transactions.gasPrice as tx_gasPrice",
                "transactions.hash as tx_hash",
                "transactions.input as tx_input",
                "transactions.maxFeePerGas as tx_maxFeePerGas",
                "transactions.maxPriorityFeePerGas as tx_maxPriorityFeePerGas",
                "transactions.nonce as tx_nonce",
                "transactions.r as tx_r",
                "transactions.s as tx_s",
                "transactions.to as tx_to",
                "transactions.transactionIndex as tx_transactionIndex",
                "transactions.type as tx_type",
                "transactions.value as tx_value",
                "transactions.v as tx_v",
              ]),
          )
          .$if(shouldJoinTraces, (qb) =>
            qb
              .leftJoin("callTraces", "callTraces.id", "events.callTrace_id")
              .select([
                "callTraces.id as callTrace_id",
                "callTraces.callType as callTrace_callType",
                "callTraces.from as callTrace_from",
                "callTraces.gas as callTrace_gas",
                "callTraces.input as callTrace_input",
                "callTraces.to as callTrace_to",
                "callTraces.value as callTrace_value",
                "callTraces.blockHash as callTrace_blockHash",
                "callTraces.blockNumber as callTrace_blockNumber",
                "callTraces.gasUsed as callTrace_gasUsed",
                "callTraces.output as callTrace_output",
                "callTraces.subtraces as callTrace_subtraces",
                "callTraces.traceAddress as callTrace_traceAddress",
                "callTraces.transactionHash as callTrace_transactionHash",
                "callTraces.transactionPosition as callTrace_transactionPosition",
                "callTraces.chainId as callTrace_chainId",
                "callTraces.checkpoint as callTrace_checkpoint",
              ]),
          )
          .$if(shouldJoinReceipts, (qb) =>
            qb
              .leftJoin(
                "transactionReceipts",
                "transactionReceipts.transactionHash",
                "events.transactionHash",
              )
              .select([
                "transactionReceipts.blockHash as txr_blockHash",
                "transactionReceipts.blockNumber as txr_blockNumber",
                "transactionReceipts.contractAddress as txr_contractAddress",
                "transactionReceipts.cumulativeGasUsed as txr_cumulativeGasUsed",
                "transactionReceipts.effectiveGasPrice as txr_effectiveGasPrice",
                "transactionReceipts.from as txr_from",
                "transactionReceipts.gasUsed as txr_gasUsed",
                "transactionReceipts.logs as txr_logs",
                "transactionReceipts.logsBloom as txr_logsBloom",
                "transactionReceipts.status as txr_status",
                "transactionReceipts.to as txr_to",
                "transactionReceipts.transactionHash as txr_transactionHash",
                "transactionReceipts.transactionIndex as txr_transactionIndex",
                "transactionReceipts.type as txr_type",
              ]),
          )
          .where("events.checkpoint", ">", cursor)
          .where(
            "events.checkpoint",
            "<=",
            // Get max checkpoint from all sources
            sql`(
              select max_checkpoint
              from max_checkpoint
            )`,
          )
          .orderBy("events.checkpoint", "asc")
          .limit(limit + 1)
          .execute();

        return requestedLogs.map((_row) => {
          // Without this cast, the block_ and tx_ fields are all nullable
          // which makes this very annoying. Should probably add a runtime check
          // that those fields are indeed present before continuing here.
          const row = _row as NonNull<(typeof requestedLogs)[number]>;

          const source = sourcesById[row.source_id];

          const shouldIncludeLog =
            sourceIsLog(source) || sourceIsFactoryLog(source);
          const shouldIncludeTransaction =
            sourceIsLog(source) ||
            sourceIsFactoryLog(source) ||
            sourceIsCallTrace(source) ||
            sourceIsFactoryCallTrace(source);
          const shouldIncludeTrace =
            sourceIsCallTrace(source) || sourceIsFactoryCallTrace(source);
          const shouldIncludeTransactionReceipt =
            (sourceIsLog(source) &&
              source.criteria.includeTransactionReceipts) ||
            (sourceIsFactoryLog(source) &&
              source.criteria.includeTransactionReceipts);
          return {
            chainId: source.chainId,
            sourceId: row.source_id,
            encodedCheckpoint: row.checkpoint,
            log: shouldIncludeLog
              ? {
                  address: checksumAddress(row.log_address),
                  blockHash: row.log_blockHash,
                  blockNumber: row.log_blockNumber,
                  data: row.log_data,
                  id: row.log_id as Log["id"],
                  logIndex: Number(row.log_logIndex),
                  removed: false,
                  topics: [
                    row.log_topic0,
                    row.log_topic1,
                    row.log_topic2,
                    row.log_topic3,
                  ].filter((t): t is Hex => t !== null) as [Hex, ...Hex[]] | [],
                  transactionHash: row.log_transactionHash,
                  transactionIndex: Number(row.log_transactionIndex),
                }
              : undefined,
            block: {
              baseFeePerGas: row.block_baseFeePerGas,
              difficulty: row.block_difficulty,
              extraData: row.block_extraData,
              gasLimit: row.block_gasLimit,
              gasUsed: row.block_gasUsed,
              hash: row.block_hash,
              logsBloom: row.block_logsBloom,
              miner: checksumAddress(row.block_miner),
              mixHash: row.block_mixHash,
              nonce: row.block_nonce,
              number: row.block_number,
              parentHash: row.block_parentHash,
              receiptsRoot: row.block_receiptsRoot,
              sha3Uncles: row.block_sha3Uncles,
              size: row.block_size,
              stateRoot: row.block_stateRoot,
              timestamp: row.block_timestamp,
              totalDifficulty: row.block_totalDifficulty,
              transactionsRoot: row.block_transactionsRoot,
            },
            transaction: shouldIncludeTransaction
              ? {
                  blockHash: row.tx_blockHash,
                  blockNumber: row.tx_blockNumber,
                  from: checksumAddress(row.tx_from),
                  gas: row.tx_gas,
                  hash: row.tx_hash,
                  input: row.tx_input,
                  nonce: Number(row.tx_nonce),
                  r: row.tx_r,
                  s: row.tx_s,
                  to: row.tx_to ? checksumAddress(row.tx_to) : row.tx_to,
                  transactionIndex: Number(row.tx_transactionIndex),
                  value: row.tx_value,
                  v: row.tx_v,
                  ...(row.tx_type === "0x0"
                    ? { type: "legacy", gasPrice: row.tx_gasPrice }
                    : row.tx_type === "0x1"
                      ? {
                          type: "eip2930",
                          gasPrice: row.tx_gasPrice,
                          accessList: JSON.parse(row.tx_accessList),
                        }
                      : row.tx_type === "0x2"
                        ? {
                            type: "eip1559",
                            maxFeePerGas: row.tx_maxFeePerGas,
                            maxPriorityFeePerGas: row.tx_maxPriorityFeePerGas,
                          }
                        : row.tx_type === "0x7e"
                          ? {
                              type: "deposit",
                              maxFeePerGas: row.tx_maxFeePerGas ?? undefined,
                              maxPriorityFeePerGas:
                                row.tx_maxPriorityFeePerGas ?? undefined,
                            }
                          : { type: row.tx_type }),
                }
              : undefined,
            trace: shouldIncludeTrace
              ? {
                  id: row.callTrace_id,
                  from: checksumAddress(row.callTrace_from),
                  to: checksumAddress(row.callTrace_to),
                  gas: row.callTrace_gas,
                  value: row.callTrace_value,
                  input: row.callTrace_input,
                  output: row.callTrace_output,
                  gasUsed: row.callTrace_gasUsed,
                  subtraces: row.callTrace_subtraces,
                  traceAddress: JSON.parse(row.callTrace_traceAddress),
                  blockHash: row.callTrace_blockHash,
                  blockNumber: row.callTrace_blockNumber,
                  transactionHash: row.callTrace_transactionHash,
                  transactionIndex: row.callTrace_transactionPosition,
                  callType: row.callTrace_callType as CallTrace["callType"],
                }
              : undefined,
            transactionReceipt: shouldIncludeTransactionReceipt
              ? {
                  blockHash: row.txr_blockHash,
                  blockNumber: row.txr_blockNumber,
                  contractAddress: row.txr_contractAddress
                    ? checksumAddress(row.txr_contractAddress)
                    : null,
                  cumulativeGasUsed: row.txr_cumulativeGasUsed,
                  effectiveGasPrice: row.txr_effectiveGasPrice,
                  from: checksumAddress(row.txr_from),
                  gasUsed: row.txr_gasUsed,
                  logs: JSON.parse(row.txr_logs).map((log: SyncLog) => ({
                    address: checksumAddress(log.address),
                    blockHash: log.blockHash,
                    blockNumber: hexToBigInt(log.blockNumber),
                    data: log.data,
                    logIndex: hexToNumber(log.logIndex),
                    removed: false,
                    topics: [
                      log.topics[0] ?? null,
                      log.topics[1] ?? null,
                      log.topics[2] ?? null,
                      log.topics[3] ?? null,
                    ].filter((t): t is Hex => t !== null) as
                      | [Hex, ...Hex[]]
                      | [],
                    transactionHash: log.transactionHash,
                    transactionIndex: hexToNumber(log.transactionIndex),
                  })),
                  logsBloom: row.txr_logsBloom,
                  status:
                    row.txr_status === "0x1"
                      ? "success"
                      : row.txr_status === "0x0"
                        ? "reverted"
                        : (row.txr_status as TransactionReceipt["status"]),
                  to: row.txr_to ? checksumAddress(row.txr_to) : null,
                  transactionHash: row.txr_transactionHash,
                  transactionIndex: Number(row.txr_transactionIndex),
                  type:
                    row.txr_type === "0x0"
                      ? "legacy"
                      : row.txr_type === "0x1"
                        ? "eip2930"
                        : row.tx_type === "0x2"
                          ? "eip1559"
                          : row.tx_type === "0x7e"
                            ? "deposit"
                            : row.tx_type,
                }
              : undefined,
          } satisfies RawEvent;
        });
      });

      const hasNextPage = events.length === limit + 1;

      if (!hasNextPage) {
        yield events;
        break;
      } else {
        events.pop();
        cursor = events[events.length - 1].encodedCheckpoint;
        yield events;
      }
    }
  }

  async getLastEventCheckpoint({
    sources,
    fromCheckpoint,
    toCheckpoint,
  }: {
    sources: EventSource[];
    fromCheckpoint: Checkpoint;
    toCheckpoint: Checkpoint;
  }): Promise<Checkpoint | undefined> {
    return this.db.wrap({ method: "getLastEventCheckpoint" }, async () => {
      const checkpoint = await this.db
        .selectFrom("logs")
        .where((eb) => {
          const logFilterCmprs = sources
            .filter(sourceIsLog)
            .map((logFilter) => {
              const exprs = this.buildLogFilterCmprs({ eb, logFilter });
              return eb.and(exprs);
            });

          const factoryCmprs = sources
            .filter(sourceIsFactoryLog)
            .map((factory) => {
              const exprs = this.buildFactoryLogFilterCmprs({ eb, factory });
              return eb.and(exprs);
            });

          return eb.or([...logFilterCmprs, ...factoryCmprs]);
        })
        .select("checkpoint")
        .unionAll(
          this.db
            .selectFrom("blocks")
            .where((eb) => {
              const exprs = [];
              const blockFilters = sources.filter(sourceIsBlock);
              for (const blockFilter of blockFilters) {
                exprs.push(
                  eb.and([
                    eb("chainId", "=", blockFilter.chainId),
                    eb("number", ">=", BigInt(blockFilter.startBlock)),
                    ...(blockFilter.endBlock !== undefined
                      ? [eb("number", "<=", BigInt(blockFilter.endBlock))]
                      : []),
                    sql`(number - ${sql.val(
                      BigInt(blockFilter.criteria.offset),
                    )}) % ${sql.val(
                      BigInt(blockFilter.criteria.interval),
                    )} = 0`,
                  ]),
                );
              }
              return eb.or(exprs);
            })
            .select("checkpoint"),
        )
        .unionAll(
          this.db
            .selectFrom("callTraces")
            .where((eb) => {
              const traceFilterCmprs = sources
                .filter(sourceIsCallTrace)
                .map((callTraceSource) => {
                  const exprs = this.buildTraceFilterCmprs({
                    eb,
                    callTraceSource,
                  });
                  return eb.and(exprs);
                });

              const factoryCallTraceCmprs = sources
                .filter(sourceIsFactoryCallTrace)
                .map((factory) => {
                  const exprs = this.buildFactoryTraceFilterCmprs({
                    eb,
                    factory,
                  });
                  return eb.and(exprs);
                });

              return eb.or([...traceFilterCmprs, ...factoryCallTraceCmprs]);
            })
            .select("checkpoint"),
        )
        .where("checkpoint", ">", encodeCheckpoint(fromCheckpoint))
        .where("checkpoint", "<=", encodeCheckpoint(toCheckpoint))
        .orderBy("checkpoint", "desc")
        .executeTakeFirst();

      return checkpoint
        ? checkpoint.checkpoint
          ? decodeCheckpoint(checkpoint.checkpoint)
          : undefined
        : undefined;
    });
  }

  private buildLogFilterCmprs = ({
    eb,
    logFilter,
  }: {
    eb: ExpressionBuilder<any, any>;
    logFilter: LogSource;
  }) => {
    const exprs = [];

    exprs.push(
      eb(
        "logs.chainId",
        "=",
        sql`cast (${sql.val(logFilter.chainId)} as numeric(16, 0))`,
      ),
    );

    if (logFilter.criteria.address) {
      // If it's an array of length 1, collapse it.
      const address =
        Array.isArray(logFilter.criteria.address) &&
        logFilter.criteria.address.length === 1
          ? logFilter.criteria.address[0]
          : logFilter.criteria.address;
      if (Array.isArray(address)) {
        exprs.push(eb.or(address.map((a) => eb("logs.address", "=", a))));
      } else {
        exprs.push(eb("logs.address", "=", address));
      }
    }

    if (logFilter.criteria.topics) {
      for (const idx_ of range(0, 4)) {
        const idx = idx_ as 0 | 1 | 2 | 3;
        // If it's an array of length 1, collapse it.
        const raw = logFilter.criteria.topics[idx] ?? null;
        if (raw === null) continue;
        const topic = Array.isArray(raw) && raw.length === 1 ? raw[0] : raw;
        if (Array.isArray(topic)) {
          exprs.push(eb.or(topic.map((a) => eb(`logs.topic${idx}`, "=", a))));
        } else {
          exprs.push(eb(`logs.topic${idx}`, "=", topic));
        }
      }
    }

    if (logFilter.startBlock !== undefined && logFilter.startBlock !== 0)
      exprs.push(eb("logs.blockNumber", ">=", BigInt(logFilter.startBlock)));
    if (logFilter.endBlock)
      exprs.push(eb("logs.blockNumber", "<=", BigInt(logFilter.endBlock)));

    return exprs;
  };

  private buildFactoryLogFilterCmprs = ({
    eb,
    factory,
  }: {
    eb: ExpressionBuilder<any, any>;
    factory: FactoryLogSource;
  }) => {
    const exprs = [];

    exprs.push(
      eb(
        "logs.chainId",
        "=",
        sql`cast (${sql.val(factory.chainId)} as numeric(16, 0))`,
      ),
    );

    const selectChildAddressExpression =
      buildFactoryChildAddressSelectExpression({
        childAddressLocation: factory.criteria.childAddressLocation,
      });

    exprs.push(
      eb(
        "logs.address",
        "in",
        eb
          .selectFrom("logs")
          .select(selectChildAddressExpression.as("childAddress"))
          .where("chainId", "=", factory.chainId)
          .where("address", "=", factory.criteria.address)
          .where("topic0", "=", factory.criteria.eventSelector),
      ),
    );

    if (factory.criteria.topics) {
      for (const idx_ of range(0, 4)) {
        const idx = idx_ as 0 | 1 | 2 | 3;
        // If it's an array of length 1, collapse it.
        const raw = factory.criteria.topics[idx] ?? null;
        if (raw === null) continue;
        const topic = Array.isArray(raw) && raw.length === 1 ? raw[0] : raw;
        if (Array.isArray(topic)) {
          exprs.push(eb.or(topic.map((a) => eb(`logs.topic${idx}`, "=", a))));
        } else {
          exprs.push(eb(`logs.topic${idx}`, "=", topic));
        }
      }
    }

    if (factory.startBlock !== undefined && factory.startBlock !== 0)
      exprs.push(eb("logs.blockNumber", ">=", BigInt(factory.startBlock)));
    if (factory.endBlock)
      exprs.push(eb("logs.blockNumber", "<=", BigInt(factory.endBlock)));

    return exprs;
  };

  private buildTraceFilterCmprs = ({
    eb,
    callTraceSource,
  }: {
    eb: ExpressionBuilder<any, any>;
    callTraceSource: CallTraceSource;
  }) => {
    const exprs = [];

    exprs.push(
      eb(
        "callTraces.chainId",
        "=",
        sql`cast (${sql.val(callTraceSource.chainId)} as numeric(16, 0))`,
      ),
    );

    if (callTraceSource.criteria.fromAddress) {
      // If it's an array of length 1, collapse it.
      const fromAddress =
        Array.isArray(callTraceSource.criteria.fromAddress) &&
        callTraceSource.criteria.fromAddress.length === 1
          ? callTraceSource.criteria.fromAddress[0]
          : callTraceSource.criteria.fromAddress;
      if (Array.isArray(fromAddress)) {
        exprs.push(
          eb.or(fromAddress.map((a) => eb("callTraces.from", "=", a))),
        );
      } else {
        exprs.push(eb("callTraces.from", "=", fromAddress));
      }
    }

    if (callTraceSource.criteria.toAddress) {
      // If it's an array of length 1, collapse it.
      const toAddress =
        Array.isArray(callTraceSource.criteria.toAddress) &&
        callTraceSource.criteria.toAddress.length === 1
          ? callTraceSource.criteria.toAddress[0]
          : callTraceSource.criteria.toAddress;
      if (Array.isArray(toAddress)) {
        exprs.push(eb.or(toAddress.map((a) => eb("callTraces.to", "=", a))));
      } else {
        exprs.push(eb("callTraces.to", "=", toAddress));
      }
    }

    // Filter based on function selectors
    exprs.push(
      eb.or(
        callTraceSource.criteria.functionSelectors.map((fs) =>
          eb("callTraces.functionSelector", "=", fs),
        ),
      ),
    );

    // Filter out callTraces with error
    exprs.push(
      sql`${sql.ref("callTraces.error")} IS NULL` as OperandExpression<SqlBool>,
    );

    if (
      callTraceSource.startBlock !== undefined &&
      callTraceSource.startBlock !== 0
    )
      exprs.push(
        eb("callTraces.blockNumber", ">=", BigInt(callTraceSource.startBlock)),
      );
    if (callTraceSource.endBlock)
      exprs.push(
        eb("callTraces.blockNumber", "<=", BigInt(callTraceSource.endBlock)),
      );

    return exprs;
  };

  private buildFactoryTraceFilterCmprs = ({
    eb,
    factory,
  }: {
    eb: ExpressionBuilder<any, any>;
    factory: FactoryCallTraceSource;
  }) => {
    const exprs = [];

    exprs.push(
      eb(
        "callTraces.chainId",
        "=",
        sql`cast (${sql.val(factory.chainId)} as numeric(16, 0))`,
      ),
    );
    const selectChildAddressExpression =
      buildFactoryChildAddressSelectExpression({
        childAddressLocation: factory.criteria.childAddressLocation,
      });

    exprs.push(
      eb(
        "callTraces.to",
        "in",
        eb
          .selectFrom("logs")
          .select(selectChildAddressExpression.as("childAddress"))
          .where("chainId", "=", factory.chainId)
          .where("address", "=", factory.criteria.address)
          .where("topic0", "=", factory.criteria.eventSelector),
      ),
    );

    if (factory.criteria.fromAddress) {
      // If it's an array of length 1, collapse it.
      const fromAddress =
        Array.isArray(factory.criteria.fromAddress) &&
        factory.criteria.fromAddress.length === 1
          ? factory.criteria.fromAddress[0]
          : factory.criteria.fromAddress;
      if (Array.isArray(fromAddress)) {
        exprs.push(
          eb.or(fromAddress.map((a) => eb("callTraces.from", "=", a))),
        );
      } else {
        exprs.push(eb("callTraces.from", "=", fromAddress));
      }
    }

    // Filter based on function selectors
    exprs.push(
      eb.or(
        factory.criteria.functionSelectors.map((fs) =>
          eb("callTraces.functionSelector", "=", fs),
        ),
      ),
    );

    // Filter out callTraces with error
    exprs.push(
      sql`${sql.ref("callTraces.error")} IS NULL` as OperandExpression<SqlBool>,
    );

    if (factory.startBlock !== undefined && factory.startBlock !== 0)
      exprs.push(
        eb("callTraces.blockNumber", ">=", BigInt(factory.startBlock)),
      );
    if (factory.endBlock)
      exprs.push(eb("callTraces.blockNumber", "<=", BigInt(factory.endBlock)));

    return exprs;
  };
}

function buildFactoryChildAddressSelectExpression({
  childAddressLocation,
}: {
  childAddressLocation: FactoryLogFilterCriteria["childAddressLocation"];
}) {
  if (childAddressLocation.startsWith("offset")) {
    const childAddressOffset = Number(childAddressLocation.substring(6));
    const start = 2 + 12 * 2 + childAddressOffset * 2 + 1;
    const length = 20 * 2;
    return sql<Hex>`'0x' || substring(data from ${start}::int for ${length}::int)`;
  } else {
    const start = 2 + 12 * 2 + 1;
    const length = 20 * 2;
    return sql<Hex>`'0x' || substring(${sql.ref(
      childAddressLocation,
    )} from ${start}::integer for ${length}::integer)`;
  }
}
