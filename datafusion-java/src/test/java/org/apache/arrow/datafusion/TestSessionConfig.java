package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Optional;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSessionConfig {
  @Test
  public void testRegisterInvalidCsvPath(@TempDir Path tempDir) throws Exception {
    try (SessionContext context = SessionContexts.create()) {
      Path filePath = tempDir.resolve("non-existent.csv");
      assertThrows(
          RuntimeException.class,
          () -> context.registerCsv("test", filePath).join(),
          "Expected an exception to be raised from an IO error");
    }
  }

  @Test
  public void testRegisterInvalidParquetPath(@TempDir Path tempDir) throws Exception {
    try (SessionContext context = SessionContexts.create()) {
      Path filePath = tempDir.resolve("non-existent.parquet");
      assertThrows(
          RuntimeException.class,
          () -> context.registerParquet("test", filePath).join(),
          "Expected an exception to be raised from an IO error");
    }
  }

  @Test
  public void testCreateSessionWithConfig() throws Exception {
    try (SessionContext context =
        SessionContexts.withConfig(
            (c) -> c.executionOptions().parquet().withEnablePageIndex(true))) {
      // Only testing we can successfully create a session context with the config
    }
  }

  @Test
  public void testParquetOptions() throws Exception {
    try (SessionConfig config = new SessionConfig()) {
      ParquetOptions parquetOptions = config.executionOptions().parquet();

      assertTrue(parquetOptions.enablePageIndex());
      parquetOptions.withEnablePageIndex(false);
      assertFalse(parquetOptions.enablePageIndex());

      assertTrue(parquetOptions.pruning());
      parquetOptions.withPruning(false);
      assertFalse(parquetOptions.pruning());

      assertTrue(parquetOptions.skipMetadata());
      parquetOptions.withSkipMetadata(false);
      assertFalse(parquetOptions.skipMetadata());

      assertFalse(parquetOptions.metadataSizeHint().isPresent());
      parquetOptions.withMetadataSizeHint(Optional.of(123L));
      Optional<Long> sizeHint = parquetOptions.metadataSizeHint();
      assertTrue(sizeHint.isPresent());
      assertEquals(123L, sizeHint.get());
      parquetOptions.withMetadataSizeHint(Optional.empty());
      assertFalse(parquetOptions.metadataSizeHint().isPresent());

      assertFalse(parquetOptions.pushdownFilters());
      parquetOptions.withPushdownFilters(true);
      assertTrue(parquetOptions.pushdownFilters());

      assertFalse(parquetOptions.reorderFilters());
      parquetOptions.withReorderFilters(true);
      assertTrue(parquetOptions.reorderFilters());
    }
  }

  @Test
  public void testSqlParserOptions() throws Exception {
    try (SessionConfig config = new SessionConfig()) {
      SqlParserOptions sqlParserOptions = config.sqlParserOptions();

      assertFalse(sqlParserOptions.parseFloatAsDecimal());
      sqlParserOptions.withParseFloatAsDecimal(true);
      assertTrue(sqlParserOptions.parseFloatAsDecimal());

      assertTrue(sqlParserOptions.enableIdentNormalization());
      sqlParserOptions.withEnableIdentNormalization(false);
      assertFalse(sqlParserOptions.enableIdentNormalization());

      assertEquals("generic", sqlParserOptions.dialect());
      sqlParserOptions.withDialect("PostgreSQL");
      assertEquals("PostgreSQL", sqlParserOptions.dialect());
    }
  }

  @Test
  public void testExecutionOptions() throws Exception {
    try (SessionConfig config = new SessionConfig()) {
      ExecutionOptions executionOptions = config.executionOptions();

      assertEquals(8192, executionOptions.batchSize());
      executionOptions.withBatchSize(1024);
      assertEquals(1024, executionOptions.batchSize());

      assertTrue(executionOptions.coalesceBatches());
      executionOptions.withCoalesceBatches(false);
      assertFalse(executionOptions.coalesceBatches());

      assertFalse(executionOptions.collectStatistics());
      executionOptions.withCollectStatistics(true);
      assertTrue(executionOptions.collectStatistics());

      long targetPartitions = executionOptions.targetPartitions();
      assertTrue(targetPartitions > 0);
      executionOptions.withTargetPartitions(targetPartitions * 2);
      assertEquals(targetPartitions * 2, executionOptions.targetPartitions());
    }
  }

  @Test
  public void testBatchSize(@TempDir Path tempDir) throws Exception {
    long rowCount = 1024;
    long batchSize = 64;
    try (SessionContext context =
            SessionContexts.withConfig((conf) -> conf.executionOptions().withBatchSize(batchSize));
        BufferAllocator allocator = new RootAllocator()) {
      Path parquetFilePath = tempDir.resolve("data.parquet");

      String parquetSchema =
          "{\"namespace\": \"org.example\","
              + "\"type\": \"record\","
              + "\"name\": \"record_name\","
              + "\"fields\": ["
              + " {\"name\": \"x\", \"type\": \"long\"}"
              + " ]}";

      ParquetWriter.writeParquet(
          parquetFilePath,
          parquetSchema,
          1024,
          (i, record) -> {
            record.put("x", i);
          });

      context.registerParquet("test", parquetFilePath).join();

      try (RecordBatchStream stream =
          context
              .sql("SELECT * FROM test")
              .thenComposeAsync(df -> df.executeStream(allocator))
              .join()) {
        VectorSchemaRoot root = stream.getVectorSchemaRoot();

        long rowsReceived = 0;
        while (stream.loadNextBatch().join()) {
          assertTrue(root.getRowCount() <= batchSize);
          rowsReceived += root.getRowCount();
        }

        assertEquals(rowCount, rowsReceived);
      }
    }
  }
}
