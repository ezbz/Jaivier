package org.projectx.hive.connection;

import org.projectx.hive.Host;

/**
 * Interface for a factory creating {@link HiveConnection connections} to a hive server
 * 
 * @author erez
 *
 */
public interface HiveConnectionFactory {

  HiveConnection createHiveConnection(Host host, int timeout);
}
