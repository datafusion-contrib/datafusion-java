package org.apache.arrow.datafusion;

interface ObjectResultCallback {
  void callback(String errMessage, long value);
}
