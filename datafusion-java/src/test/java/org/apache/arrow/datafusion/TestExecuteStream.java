package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
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
}
