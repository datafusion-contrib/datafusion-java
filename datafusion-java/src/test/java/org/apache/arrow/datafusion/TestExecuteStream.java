package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestExecuteStream {
  @Test
  public void executeStream(@TempDir Path tempDir) throws Exception {
    try (SessionContext context = SessionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {
      Path csvFilePath = tempDir.resolve("data.csv");

      List<String> lines = Arrays.asList("x,y,z", "1,2,3.5", "4,5,6.5", "7,8,9.5");
      Files.write(csvFilePath, lines);

      context.registerCsv("test", csvFilePath).join();

      try (RecordBatchStream stream =
          context
              .sql("SELECT y,z FROM test WHERE x > 3")
              .thenComposeAsync(df -> df.executeStream(allocator))
              .join()) {
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        Schema schema = root.getSchema();
        assertEquals(2, schema.getFields().size());
        assertEquals("y", schema.getFields().get(0).getName());
        assertEquals("z", schema.getFields().get(1).getName());

        assertTrue(stream.loadNextBatch().join());
        assertEquals(2, root.getRowCount());
        BigIntVector yValues = (BigIntVector) root.getVector(0);
        assertEquals(5, yValues.get(0));
        assertEquals(8, yValues.get(1));
        Float8Vector zValues = (Float8Vector) root.getVector(1);
        assertEquals(6.5, zValues.get(0));
        assertEquals(9.5, zValues.get(1));

        assertFalse(stream.loadNextBatch().join());
      }
    }
  }

  @Test
  public void readDictionaryData() throws Exception {
    try (SessionContext context = SessionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {

      URL fileUrl = this.getClass().getResource("/dictionary_data.parquet");
      Path parquetFilePath = Paths.get(fileUrl.getPath());

      context.registerParquet("test", parquetFilePath).join();

      try (RecordBatchStream stream =
          context
              .sql("SELECT x,y FROM test")
              .thenComposeAsync(df -> df.executeStream(allocator))
              .join()) {
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        Schema schema = root.getSchema();
        assertEquals(2, schema.getFields().size());
        assertEquals("x", schema.getFields().get(0).getName());
        assertEquals("y", schema.getFields().get(1).getName());

        int rowsRead = 0;
        while (stream.loadNextBatch().join()) {
          int batchNumRows = root.getRowCount();
          BigIntVector xValuesEncoded = (BigIntVector) root.getVector(0);
          long xDictionaryId = xValuesEncoded.getField().getDictionary().getId();
          try (VarCharVector xValues =
              (VarCharVector)
                  DictionaryEncoder.decode(xValuesEncoded, stream.lookup(xDictionaryId))) {
            String[] expected = {"one", "two", "three"};
            for (int i = 0; i < batchNumRows; ++i) {
              assertEquals(
                  new String(xValues.get(i), StandardCharsets.UTF_8), expected[(rowsRead + i) % 3]);
            }
          }

          BigIntVector yValuesEncoded = (BigIntVector) root.getVector(1);
          long yDictionaryId = yValuesEncoded.getField().getDictionary().getId();
          try (VarCharVector yValues =
              (VarCharVector)
                  DictionaryEncoder.decode(yValuesEncoded, stream.lookup(yDictionaryId))) {
            String[] expected = {"four", "five", "six"};
            for (int i = 0; i < batchNumRows; ++i) {
              assertEquals(
                  new String(yValues.get(i), StandardCharsets.UTF_8), expected[(rowsRead + i) % 3]);
            }
          }
          rowsRead += batchNumRows;
        }

        assertEquals(100, rowsRead);
      }
    }
  }
}
