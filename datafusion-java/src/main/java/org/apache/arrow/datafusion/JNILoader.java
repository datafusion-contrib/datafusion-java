package org.apache.arrow.datafusion;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class JNILoader {

  private JNILoader() {}

  private static final Logger logger = LoggerFactory.getLogger(JNILoader.class);

  private static final AtomicBoolean loaded = new AtomicBoolean(false);

  private enum OsName {
    Windows,
    Osx,
    Linux
  }

  private static final String libraryName = "datafusion_jni";

  private static final String ERROR_MSG =
      String.format(
          "Unsupported OS/arch (`%s' detected), cannot find `%s' or load `%s' from system libraries. "
              + "Please try building from source the jar or providing %s in your system.",
          getOsName(), getResourceName(), libraryName, libraryName);

  private static OsName getOsName() {
    String os = System.getProperty("os.name").toLowerCase().replace(' ', '_');
    if (os.contains("win")) {
      return OsName.Windows;
    } else if (os.startsWith("mac") || os.contains("os_x")) {
      return OsName.Osx;
    } else {
      return OsName.Linux;
    }
  }

  private static String getLibraryFileName() {
    String prefix = "lib";
    if (getOsName() == OsName.Windows) {
      prefix = "";
    }
    return prefix + libraryName + "." + getExtension();
  }

  /**
   * @return the absolute path in the jar file for the jni library
   */
  private static String getResourceName() {
    return "/jni_libs/" + getLibraryFileName();
  }

  private static String getExtension() {
    OsName osName = getOsName();
    if (osName == OsName.Linux) {
      return "so";
    } else if (osName == OsName.Osx) {
      return "dylib";
    } else if (osName == OsName.Windows) {
      return "dll";
    }
    throw new IllegalStateException("Cannot determine the extension for " + osName);
  }

  static synchronized void load() {
    if (loaded.get()) {
      logger.debug("{} already loaded, returning", libraryName);
      return;
    }
    InputStream is = JNILoader.class.getResourceAsStream(getResourceName());
    if (is == null) {
      try {
        System.loadLibrary(libraryName);
        loaded.set(true);
        logger.debug("successfully loaded {} from library path", libraryName);
        return;
      } catch (UnsatisfiedLinkError e) {
        UnsatisfiedLinkError err =
            new UnsatisfiedLinkError(String.format("%s\n%s", e.getMessage(), ERROR_MSG));
        err.setStackTrace(e.getStackTrace());
        throw err;
      }
    }
    final File tempFile = extractToTempFile(is);
    try {
      System.load(tempFile.getAbsolutePath());
    } catch (UnsatisfiedLinkError le1) {
      // fall-back to loading from the system library path
      try {
        System.loadLibrary(libraryName);
        logger.debug("successfully loaded {} from extracted lib file", libraryName);
        loaded.set(true);
      } catch (UnsatisfiedLinkError le2) {
        // display error in case problem with loading from temp folder
        // and from system library path - concatenate both messages
        UnsatisfiedLinkError err =
            new UnsatisfiedLinkError(
                String.format("%s\n%s\n%s", le1.getMessage(), le2.getMessage(), ERROR_MSG));
        err.setStackTrace(le2.getStackTrace());
        throw err;
      }
    }
  }

  private static File extractToTempFile(InputStream is) {
    final File tempFile;
    try {
      tempFile = File.createTempFile(libraryName, "." + getExtension(), null);
      tempFile.deleteOnExit();
    } catch (IOException e) {
      throw new IllegalStateException("Cannot create temporary files", e);
    }
    try (InputStream in = is;
        FileOutputStream out = new FileOutputStream(tempFile)) {
      byte[] buf = new byte[8192];
      while (true) {
        int read = in.read(buf);
        if (read == -1) {
          break;
        }
        out.write(buf, 0, read);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to extract lib file and write to temp file", e);
    }
    return tempFile;
  }
}
