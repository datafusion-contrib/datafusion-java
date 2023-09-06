package org.apache.arrow.datafusion;

class ErrorUtil {

  private ErrorUtil() {}

  static boolean containsError(String errString) {
    return errString != null && !errString.isEmpty();
  }
}
