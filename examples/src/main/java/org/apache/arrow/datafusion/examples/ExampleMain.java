package org.apache.arrow.datafusion.examples;

import org.apache.arrow.datafusion.DataFrame;
import org.apache.arrow.datafusion.ExecutionContext;
import org.apache.arrow.datafusion.ExecutionContexts;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class ExampleMain {

  private static final Logger logger = LogManager.getLogger(ExampleMain.class);

  public static void main(String[] args) throws Exception {
    try (ExecutionContext context = ExecutionContexts.create()) {
      DataFrame dataFrame = context.sql("select 1 + 2");
      logger.printf(Level.INFO, "successfully loaded data frame %s", dataFrame);

      dataFrame = context.sql("select 2");
      logger.printf(Level.INFO, "successfully loaded data frame %s", dataFrame);

      dataFrame = context.sql("select cos(2.0)");
      logger.printf(Level.INFO, "successfully loaded data frame %s", dataFrame);

      dataFrame.show().thenRun(() -> logger.info("show was run successfully"));
      dataFrame.collect().thenAccept(arrowRecordBatches -> logger.info("arrow result {}", arrowRecordBatches));
    }
  }
}
