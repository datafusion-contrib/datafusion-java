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
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleMain {

  private static final Logger logger = LoggerFactory.getLogger(ExampleMain.class);

  public static void main(String[] args) throws Exception {
    try (ExecutionContext context = ExecutionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {
      loadConstant(context).join();

      context.registerCsv("test_csv", Paths.get("src/main/resources/test_table.csv")).join();
      context.sql("select * from test_csv limit 3").thenComposeAsync(DataFrame::show).join();

      context
          .registerParquet(
              "test_parquet", Paths.get("src/main/resources/aggregate_test_100.parquet"))
          .join();
      context.sql("select * from test_parquet limit 3").thenComposeAsync(DataFrame::show).join();

      context
          .sql("select * from test_csv")
          .thenComposeAsync(df -> df.collect(allocator))
          .thenAccept(ExampleMain::consumeReader)
          .join();
    }
  }

  private static void consumeReader(ArrowReader reader) {
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
  }

  private static CompletableFuture<Void> loadConstant(ExecutionContext context) {
    return context
        .sql("select 1 + 2")
        .thenComposeAsync(
            dataFrame -> {
              logger.info("successfully loaded data frame {}", dataFrame);
              return dataFrame.show();
            });
  }
}
