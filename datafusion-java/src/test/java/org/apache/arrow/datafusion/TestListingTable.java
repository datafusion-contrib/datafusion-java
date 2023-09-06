package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestListingTable {
  @Test
  public void testCsvListingTable(@TempDir Path tempDir) throws Exception {
    try (SessionContext context = SessionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {
      Path dataDir = tempDir.resolve("data");
      Files.createDirectories(dataDir);

      Path csvFilePath0 = dataDir.resolve("0.csv");
      List<String> lines = Arrays.asList("x,y", "1,2", "3,4");
      Files.write(csvFilePath0, lines);

      Path csvFilePath1 = dataDir.resolve("1.csv");
      lines = Arrays.asList("x,y", "1,12", "3,14");
      Files.write(csvFilePath1, lines);

      try (CsvFormat format = new CsvFormat();
          ListingOptions listingOptions =
              ListingOptions.builder(format).withFileExtension(".csv").build();
          ListingTableConfig tableConfig =
              ListingTableConfig.builder(dataDir)
                  .withListingOptions(listingOptions)
                  .build(context)
                  .join();
          ListingTable listingTable = new ListingTable(tableConfig)) {
        context.registerTable("test", listingTable);
        testQuery(context, allocator);
      }
    }
  }

  @Test
  public void testParquetListingTable(@TempDir Path tempDir) throws Exception {
    try (SessionContext context = SessionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {
      Path dataDir = tempDir.resolve("data");
      writeParquetFiles(dataDir);

      try (ParquetFormat format = new ParquetFormat();
          ListingOptions listingOptions =
              ListingOptions.builder(format).withFileExtension(".parquet").build();
          ListingTableConfig tableConfig =
              ListingTableConfig.builder(dataDir)
                  .withListingOptions(listingOptions)
                  .build(context)
                  .join();
          ListingTable listingTable = new ListingTable(tableConfig)) {
        context.registerTable("test", listingTable);
        testQuery(context, allocator);
      }
    }
  }

  @Test
  public void testDisableCollectStat(@TempDir Path tempDir) throws Exception {
    try (SessionContext context = SessionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {
      Path dataDir = tempDir.resolve("data");
      writeParquetFiles(dataDir);

      try (ParquetFormat format = new ParquetFormat();
          ListingOptions listingOptions =
              ListingOptions.builder(format)
                  .withFileExtension(".parquet")
                  .withCollectStat(false)
                  .build();
          ListingTableConfig tableConfig =
              ListingTableConfig.builder(dataDir)
                  .withListingOptions(listingOptions)
                  .build(context)
                  .join();
          ListingTable listingTable = new ListingTable(tableConfig)) {
        context.registerTable("test", listingTable);
        testQuery(context, allocator);
      }
    }
  }

  @Test
  public void testMultiplePaths(@TempDir Path tempDir) throws Exception {
    try (SessionContext context = SessionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {
      Path dataDir = tempDir.resolve("data");
      Path[] dataFiles = writeParquetFiles(dataDir);

      try (ParquetFormat format = new ParquetFormat();
          ListingOptions listingOptions =
              ListingOptions.builder(format).withFileExtension(".parquet").build();
          ListingTableConfig tableConfig =
              ListingTableConfig.builder(dataFiles)
                  .withListingOptions(listingOptions)
                  .build(context)
                  .join();
          ListingTable listingTable = new ListingTable(tableConfig)) {
        context.registerTable("test", listingTable);
        testQuery(context, allocator);
      }
    }
  }

  private static Path[] writeParquetFiles(Path dataDir) throws Exception {
    String schema =
        "{\"namespace\": \"org.example\","
            + "\"type\": \"record\","
            + "\"name\": \"record_name\","
            + "\"fields\": ["
            + " {\"name\": \"x\", \"type\": \"long\"},"
            + " {\"name\": \"y\", \"type\": \"long\"}"
            + " ]}";

    Path parquetFilePath0 = dataDir.resolve("0.parquet");
    ParquetWriter.writeParquet(
        parquetFilePath0,
        schema,
        2,
        (i, record) -> {
          record.put("x", i * 2 + 1);
          record.put("y", i * 2 + 2);
        });

    Path parquetFilePath1 = dataDir.resolve("1.parquet");
    ParquetWriter.writeParquet(
        parquetFilePath1,
        schema,
        2,
        (i, record) -> {
          record.put("x", i * 2 + 1);
          record.put("y", i * 2 + 12);
        });
    return new Path[] {parquetFilePath0, parquetFilePath1};
  }

  private static void testQuery(SessionContext context, BufferAllocator allocator)
      throws Exception {
    try (ArrowReader reader =
        context
            .sql("SELECT y FROM test WHERE x = 3 ORDER BY y")
            .thenComposeAsync(df -> df.collect(allocator))
            .join()) {

      long[] expectedResults = {4, 14};
      int globalRow = 0;
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      while (reader.loadNextBatch()) {
        BigIntVector yValues = (BigIntVector) root.getVector(0);
        for (int row = 0; row < root.getRowCount(); ++row, ++globalRow) {
          assertTrue(globalRow < expectedResults.length);
          assertEquals(expectedResults[globalRow], yValues.get(row));
        }
      }
      assertEquals(expectedResults.length, globalRow);
    }
  }

  @Test
  public void testParquetTimestampedStrings(@TempDir Path tempDir) throws Exception {
    try (SessionContext context = SessionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {
      Path dataDir = tempDir.resolve("data");
      String schema =
          "{\"namespace\": \"org.example\","
              + "\"type\": \"record\","
              + "\"name\": \"record_name\","
              + "\"fields\": ["
              + " {\"name\": \"id\", \"type\": \"long\"},"
              + " {\"name\": \"timestamp\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}},"
              + " {\"name\": \"text\", \"type\": \"string\"}"
              + " ]}";

      Path parquetFilePath0 = dataDir.resolve("0.parquet");
      Instant[] timestamps0 = {
        Instant.parse("2022-04-04T00:00:00Z"),
        Instant.parse("2022-05-04T00:00:00Z"),
        Instant.parse("2022-06-06T00:00:00Z"),
      };
      ParquetWriter.writeParquet(
          parquetFilePath0,
          schema,
          3,
          (i, record) -> {
            record.put("id", i + 1);
            record.put("timestamp", (timestamps0[i].getEpochSecond() * 1_000));
            record.put("text", String.format("Text%d", i + 1));
          });

      Path parquetFilePath1 = dataDir.resolve("1.parquet");
      Instant[] timestamps1 = {
        Instant.parse("2023-04-04T00:00:00Z"),
        Instant.parse("2023-04-04T00:00:00Z"),
        Instant.parse("2022-08-01T00:00:00Z"),
      };
      ParquetWriter.writeParquet(
          parquetFilePath1,
          schema,
          3,
          (i, record) -> {
            record.put("id", i + 4);
            record.put("timestamp", (timestamps1[i].getEpochSecond() * 1_000));
            record.put("text", String.format("Text%d", i + 4));
          });

      Path[] filePaths = {parquetFilePath0, parquetFilePath1};

      try (ParquetFormat format = new ParquetFormat();
          ListingOptions listingOptions =
              ListingOptions.builder(format).withFileExtension(".parquet").build();
          ListingTableConfig tableConfig =
              ListingTableConfig.builder(filePaths)
                  .withListingOptions(listingOptions)
                  .build(context)
                  .join();
          ListingTable listingTable = new ListingTable(tableConfig)) {
        context.registerTable("test", listingTable);
        try (ArrowReader reader =
            context
                .sql(
                    "SELECT id,text FROM test WHERE ID IN (2, 3, 4) AND timestamp < '2023-01-01T00:00:00Z' ORDER BY id")
                .thenComposeAsync(df -> df.collect(allocator))
                .join()) {

          Long[] expectedIds = {2L, 3L};
          String[] expectedText = {"Text2", "Text3"};
          List<Long> actualIds = new ArrayList<>();
          List<String> actualText = new ArrayList<>();
          int globalRow = 0;
          VectorSchemaRoot root = reader.getVectorSchemaRoot();
          while (reader.loadNextBatch()) {
            BigIntVector idValues = (BigIntVector) root.getVector(0);
            VarCharVector textValues = (VarCharVector) root.getVector(1);
            for (int row = 0; row < root.getRowCount(); ++row, ++globalRow) {
              actualIds.add(idValues.get(row));
              actualText.add(new String(textValues.get(row), StandardCharsets.UTF_8));
            }
          }
          assertArrayEquals(expectedIds, actualIds.toArray(new Long[0]));
          assertArrayEquals(expectedText, actualText.toArray(new String[0]));
        }
      }
    }
  }
}
