package org.projectx.hive.connection;

import org.projectx.hive.Host;
import org.projectx.thrift.ConfigurableThriftProtocolFactory;
import org.projectx.thrift.DefaultThriftSocketFactory;
import org.projectx.thrift.ThriftProtocolFactory;
import org.projectx.thrift.ThriftSocketFactory;
import org.springframework.util.Assert;


public class ManagedHiveConnectionFactory implements HiveConnectionFactory {

  private final ThriftSocketFactory thriftSocketFactory;
  private final ThriftProtocolFactory thriftProtocolFactory;
  private final HiveClientFactory hiveClientFactory;

  public ManagedHiveConnectionFactory(final ThriftSocketFactory thriftSocketFactory, final ThriftProtocolFactory thriftProtocolFactory,
      final HiveClientFactory hiveClientFactory) {
    Assert.notNull(thriftSocketFactory, "thriftSocketFactory cannot be null");
    Assert.notNull(thriftProtocolFactory, "thriftProtocolFactory cannot be null");
    Assert.notNull(hiveClientFactory, "hiveClientFactory cannot be null");

    this.thriftSocketFactory = thriftSocketFactory;
    this.thriftProtocolFactory = thriftProtocolFactory;
    this.hiveClientFactory = hiveClientFactory;
  }

  public ManagedHiveConnectionFactory() {
    thriftSocketFactory = new DefaultThriftSocketFactory();
    thriftProtocolFactory = new ConfigurableThriftProtocolFactory();
    hiveClientFactory = new DefaultHiveClientFactory();
  }

  @Override
  public HiveConnection createHiveConnection(final Host host, final int timeout) {
    return new ManagedHiveConnection(thriftSocketFactory, thriftProtocolFactory, hiveClientFactory, host, timeout);
  }

}
