package org.projectx.hive.connection;

import org.apache.hadoop.hive.service.HiveClient;
import org.apache.thrift.protocol.TProtocol;

/**
 * Default {@link HiveClientFactory} implementation creating a simple {@link HiveClient}
 * 
 * @author erez
 *
 */
public class DefaultHiveClientFactory implements HiveClientFactory {

  @Override
  public HiveClient createHiveClient(final TProtocol protocol) {
    return new HiveClient(protocol);
  }

}
