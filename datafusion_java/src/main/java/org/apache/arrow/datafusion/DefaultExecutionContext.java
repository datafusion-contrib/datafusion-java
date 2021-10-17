package org.apache.arrow.datafusion;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class DefaultExecutionContext extends AbstractProxy implements ExecutionContext {

  private static final Logger logger = LogManager.getLogger(DefaultExecutionContext.class);

  static native long querySql(
      DefaultExecutionContext self, long contextId, long invocationId, String sql);

  public void onErrorMessage(long invocationId, String errorMessage) {
    String oldError = errorMessageInbox.put(invocationId, errorMessage);
    assert oldError == null : "impossibly got duplicated invocation id";
  }

  @Override
  public DataFrame sql(String sql) {
    long invocationId = ThreadLocalRandom.current().nextLong();
    long dataFramePointerId = querySql(this, getPointer(), invocationId, sql);
    if (dataFramePointerId <= 0) {
      throw getErrorForInvocation(invocationId);
    } else {
      DefaultDataFrame frame = new DefaultDataFrame(this, dataFramePointerId);
      registerChild(frame);
      return frame;
    }
  }

  @Override
  public Runtime getRuntime() {
    return runtime;
  }

  private RuntimeException getErrorForInvocation(long invocationId) {
    String errorMessage = errorMessageInbox.remove(invocationId);
    assert errorMessage != null : "onErrorMessage was not properly called from JNI";
    return new RuntimeException(errorMessage);
  }

  private final TokioRuntime runtime;
  private final ConcurrentMap<Long, String> errorMessageInbox;

  DefaultExecutionContext(long pointer) {
    super(pointer);
    this.runtime = TokioRuntime.create();
    registerChild(runtime);
    this.errorMessageInbox = new ConcurrentHashMap<>();
  }

  @Override
  void doClose(long pointer) throws Exception {
    ExecutionContexts.destroyExecutionContext(pointer);
  }
}
