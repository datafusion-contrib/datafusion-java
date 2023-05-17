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
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestExecuteStream {
  @Test
  public void executeStream(@TempDir Path tempDir) throws Exception {
    try (SessionContext context = SessionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {
      Path csvFilePath = tempDir.resolve("data.csv");

      List<String> lines = Arrays.asList("x,y", "1,2", "3,4");
      Files.write(csvFilePath, lines);

      context.registerCsv("test", csvFilePath).join();

      try (RecordBatchStream stream =
          context
              .sql("SELECT y FROM test WHERE x = 3")
              .thenComposeAsync(df -> df.executeStream(allocator))
              .join()) {
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        Schema schema = root.getSchema();
        assertEquals(1, schema.getFields().size());
        assertEquals("y", schema.getFields().get(0).getName());

        assertTrue(stream.loadNextBatch().join());
        assertEquals(1, root.getRowCount());
        BigIntVector yValues = (BigIntVector) root.getVector(0);
        assertEquals(4, yValues.get(0));

        assertFalse(stream.loadNextBatch().join());
      }
    }
  }
}
