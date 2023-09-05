package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
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
}
