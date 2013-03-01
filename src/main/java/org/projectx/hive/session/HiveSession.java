package org.projectx.hive.session;

import org.projectx.hive.connection.HiveConnection;

/**
 * Interface for a hive session which is used to create a wrapper for a <code>HiveClient</code> class, 
 * this wrapper is a configured {@link HiveConnection} instances used to execute hive operations.
 * 
 * 
 * @author erez
 */
public interface HiveSession {
  /**
   * Create a connection to hive
   * @return a {@link HiveConnection} instance for execution of hive operations
   */
  HiveConnection createConnection();
}
