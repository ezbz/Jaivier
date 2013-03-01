package org.projectx.hive.connection;

import org.projectx.hive.Host;

public class TestHiveConnectionFactory implements HiveConnectionFactory {

  @Override
  public HiveConnection createHiveConnection(final Host host, final int timeout) {
    return new MemoryHiveConnection();
  }

}
