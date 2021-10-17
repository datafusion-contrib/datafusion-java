package org.apache.arrow.datafusion.examples;

import java.io.IOException;
import org.apache.arrow.datafusion.DataFrame;
import org.apache.arrow.datafusion.ExecutionContext;
import org.apache.arrow.datafusion.ExecutionContexts;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExampleMain {

  private static final Logger logger = LogManager.getLogger(ExampleMain.class);

  public static void main(String[] args) throws Exception {
    try (ExecutionContext context = ExecutionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {
      DataFrame dataFrame = context.sql("select 1 + 2");
      logger.printf(Level.INFO, "successfully loaded data frame %s", dataFrame);

      dataFrame = context.sql("select 2");
      logger.printf(Level.INFO, "successfully loaded data frame %s", dataFrame);

      dataFrame = context.sql("select cos(2.0)");
      logger.printf(Level.INFO, "successfully loaded data frame %s", dataFrame);

      dataFrame.show().thenRun(() -> logger.info("show was run successfully"));
      dataFrame.collect(allocator).thenAccept(ExampleMain::onReaderResult);
    }
  }

  private static void onReaderResult(ArrowReader reader) {
    try {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      logger.info("schema {}, bytes read={}", schema, reader.bytesRead());
      while (reader.loadNextBatch()) {
        logger.info("loading next batch");
        Float8Vector vector = (Float8Vector) root.getVector(0);
        logger.info("vector size {}, row count {}", vector.getValueCount(), root.getRowCount());
        for (int i = 0; i < root.getRowCount(); i += 1) {
          logger.info("value {}={}", i, vector.getValueAsDouble(i));
        }
      }
      reader.close();
    } catch (IOException e) {
      logger.warn("got IO Exception", e);
    }
  }
}
