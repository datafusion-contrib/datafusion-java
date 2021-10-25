package org.apache.arrow.datafusion.examples;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import org.apache.arrow.datafusion.DataFrame;
import org.apache.arrow.datafusion.ExecutionContext;
import org.apache.arrow.datafusion.ExecutionContexts;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExampleMain {

  private static final Logger logger = LogManager.getLogger(ExampleMain.class);

  public static void main(String[] args) throws Exception {
    try (ExecutionContext context = ExecutionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {
      context
          .sql("select 1 + 2")
          .thenAccept(
              dataFrame ->
                  logger.printf(Level.INFO, "successfully loaded data frame %s", dataFrame))
          .join();
      context
          .sql("select 2")
          .thenAccept(
              dataFrame ->
                  logger.printf(Level.INFO, "successfully loaded data frame %s", dataFrame))
          .join();
      CompletableFuture<DataFrame> future = context.sql("select cos(2.0)");
      future
          .thenComposeAsync(
              dataFrame -> {
                logger.printf(Level.INFO, "successfully loaded data frame %s", dataFrame);
                return dataFrame.show();
              })
          .thenRun(() -> logger.info("show was run successfully"))
          .join();
      future
          .thenComposeAsync(df -> df.collect(allocator))
          .thenAccept(
              reader -> {
                try {
                  VectorSchemaRoot root = reader.getVectorSchemaRoot();
                  while (reader.loadNextBatch()) {
                    Float8Vector result = (Float8Vector) root.getVector(0);
                    logger.info(
                        "name vector size {}, row count {}, value={}",
                        result.getValueCount(),
                        root.getRowCount(),
                        result);
                  }
                  reader.close();
                } catch (IOException e) {
                  logger.warn("got IO Exception", e);
                }
              })
          .join();

      context.registerCsv("test_csv", Paths.get("src/main/resources/test_table.csv")).join();
      context.sql("select * from test_csv").thenComposeAsync(DataFrame::show).join();

      context
          .sql("select * from test_csv")
          .thenComposeAsync(df -> df.collect(allocator))
          .thenAccept(
              reader -> {
                try {
                  VectorSchemaRoot root = reader.getVectorSchemaRoot();
                  while (reader.loadNextBatch()) {
                    VarCharVector nameVector = (VarCharVector) root.getVector(0);
                    logger.info(
                        "name vector size {}, row count {}, value={}",
                        nameVector.getValueCount(),
                        root.getRowCount(),
                        nameVector);
                    BigIntVector ageVector = (BigIntVector) root.getVector(1);
                    logger.info(
                        "age vector size {}, row count {}, value={}",
                        ageVector.getValueCount(),
                        root.getRowCount(),
                        ageVector);
                  }
                  reader.close();
                } catch (IOException e) {
                  logger.warn("got IO Exception", e);
                }
              })
          .join();
    }
  }
}
