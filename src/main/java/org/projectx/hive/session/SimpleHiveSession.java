package org.projectx.hive.session;

import org.projectx.hive.Host;
import org.projectx.hive.connection.HiveConnection;
import org.projectx.hive.connection.HiveConnectionFactory;
import org.springframework.util.Assert;


/**
 * A simple implementation of the {@link HiveSession} interface associated with a single host and a single connection
 * 
 * @author erez
 *
 */
public class SimpleHiveSession implements HiveSession {

  private final Host host;
  private final int timeout;
  private final HiveConnectionFactory hiveConnectionFactory;

  public SimpleHiveSession(final HiveConnectionFactory hiveConnectionFactory, final Host host, final int timeout) {
    Assert.notNull(hiveConnectionFactory, "hiveConnectionFactory cannot be null");
    Assert.notNull(host, "host cannot be null");
    this.hiveConnectionFactory = hiveConnectionFactory;
    this.host = host;
    this.timeout = timeout;
  }

  @Override
  public HiveConnection createConnection() {
    final HiveConnection connection = hiveConnectionFactory.createHiveConnection(host, timeout);
    connection.open();
    return connection;
  }
}
