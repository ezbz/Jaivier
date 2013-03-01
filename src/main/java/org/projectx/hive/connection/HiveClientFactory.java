package org.projectx.hive.connection;

import org.apache.hadoop.hive.service.HiveClient;
import org.apache.thrift.protocol.TProtocol;

public interface HiveClientFactory {

  HiveClient createHiveClient(TProtocol protocol);
}
