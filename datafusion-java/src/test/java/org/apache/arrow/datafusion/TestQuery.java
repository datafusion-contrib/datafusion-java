package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestQuery {
  @Test
  public void testQueryCsv(@TempDir Path tempDir) throws Exception {
    try (SessionContext context = SessionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {
      Path csvFilePath = tempDir.resolve("data.csv");

      List<String> lines = Arrays.asList("x,y", "1,2", "3,4");
      Files.write(csvFilePath, lines);

      context.registerCsv("test", csvFilePath).join();
      testQuery(context, allocator);
    }
  }

  @Test
  public void testQueryParquet(@TempDir Path tempDir) throws Exception {
    try (SessionContext context = SessionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {
      Path parquetFilePath = tempDir.resolve("data.parquet");

      String schema =
          "{\"namespace\": \"org.example\","
              + "\"type\": \"record\","
              + "\"name\": \"record_name\","
              + "\"fields\": ["
              + " {\"name\": \"x\", \"type\": \"long\"},"
              + " {\"name\": \"y\", \"type\": \"long\"}"
              + " ]}";

      ParquetWriter.writeParquet(
          parquetFilePath,
          schema,
          2,
          (i, record) -> {
            record.put("x", i * 2 + 1);
            record.put("y", i * 2 + 2);
          });

      context.registerParquet("test", parquetFilePath).join();
      testQuery(context, allocator);
    }
  }

  private static void testQuery(SessionContext context, BufferAllocator allocator)
      throws Exception {
    try (ArrowReader reader =
        context
            .sql("SELECT y FROM test WHERE x = 3")
            .thenComposeAsync(df -> df.collect(allocator))
            .join()) {

      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertTrue(reader.loadNextBatch());

      assertEquals(1, root.getRowCount());
      BigIntVector yValues = (BigIntVector) root.getVector(0);
      assertEquals(4, yValues.get(0));

      assertFalse(reader.loadNextBatch());
    }
  }
}
