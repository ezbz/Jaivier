package org.projectx.hive.connection;

import java.util.List;

import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.projectx.hive.FailFastHiveExceptionTranslator;
import org.projectx.hive.HiveDataAccessException;
import org.projectx.hive.HiveExceptionTranslator;
import org.projectx.hive.Host;
import org.projectx.thrift.ThriftProtocolFactory;
import org.projectx.thrift.ThriftSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;


/**
 * A managed implementation of the {@link HiveConnection} interface maintaining a stateful {@link TSocket} and a {@link HiveClient} instances which
 * are used to execute hive operations. Abstracts the complexity of managing the life cycle of a {@link TSocket} associated with a client 
 * throughout the execution life-cycle of hive-based operations.
 * 
 * <p>
 * 
 * <b>Important:</b> This class doesn't handle thread safety of concurrent operations on a connection (e.g., reference to a connection causes 
 * the {@link #open()} method to be invoked more than once without invoking the {@link #close()} method). It an implied responsibility of the consumer to 
 * ensure proper lifecycle management of connections.  
 * @author erez
 *
 */
public class ManagedHiveConnection implements HiveConnection {

  private static final Logger log = LoggerFactory.getLogger(ManagedHiveConnection.class);

  private TSocket socket;
  private HiveClient client;

  private final Host host;
  private final Integer timeout;

  private final HiveExceptionTranslator exceptionTranslator = new FailFastHiveExceptionTranslator();

  private final ThriftSocketFactory thriftSocketFactory;
  private final ThriftProtocolFactory thriftProtocolFactory;
  private final HiveClientFactory hiveClientFactory;

  public ManagedHiveConnection(final ThriftSocketFactory thriftSocketFactory, final ThriftProtocolFactory thriftProtocolFactory,
      final HiveClientFactory hiveClientFactory, final Host host, final Integer timeout) {
    Assert.notNull(thriftSocketFactory, "thriftSocketFactory cannot be null");
    Assert.notNull(thriftProtocolFactory, "thriftProtocolFactory cannot be null");
    Assert.notNull(hiveClientFactory, "hiveClientFactory cannot be null");
    Assert.notNull(host, "host cannot be null");
    Assert.notNull(timeout, "timeout cannot be null");
    this.thriftSocketFactory = thriftSocketFactory;
    this.thriftProtocolFactory = thriftProtocolFactory;
    this.hiveClientFactory = hiveClientFactory;
    this.host = host;
    this.timeout = timeout;
  }

  @Override
  public void open() {
    if (null != socket) {
      throw new HiveDataAccessException("Attempting to open a connection that has already been opened or not properly closed");
    }
    try {
      socket = thriftSocketFactory.createSocket(host.getHost(), host.getPort(), timeout);
      client = hiveClientFactory.createHiveClient(thriftProtocolFactory.createProtocol(socket));
      socket.open();
    } catch (final TTransportException e) {
      socket = null;
      client = null;
      throw new HiveDataAccessException("TTransportException while opening socket", e);
    }
  }

  @Override
  public boolean isOpen() {
    return (socket != null && socket.isOpen());
  }

  @Override
  public void close() {
    flush();
    if (null != socket) {
      socket.close();
    }
    client = null;
    socket = null;
  }

  @Override
  public void flush() {
    if (isOpen()) {
      try {
        socket.flush();
      } catch (final TTransportException e) {
        log.warn("Recieved a TTransportException exception during stop while calling flush", e);
      }
    }
  }

  @Override
  public void setTimeout(final int timeout) {
    if (null != socket) {
      socket.setTimeout(timeout);
    }
  }

  @Override
  public void execute(final String sql) {
    execute(new HiveClientCallback<Void>() {

      @Override
      public Void doWithClient(final HiveClient client) throws HiveServerException, TException {
        client.execute(sql);
        return null;
      }
    });
  }

  @Override
  public List<String> fetchBatch(final int batchSize) {
    return execute(new HiveClientCallback<List<String>>() {

      @Override
      public List<String> doWithClient(final HiveClient client) throws HiveServerException, TException {
        return client.fetchN(batchSize);
      }
    });
  }

  @Override
  public List<String> fetchAll() {
    return execute(new HiveClientCallback<List<String>>() {

      @Override
      public List<String> doWithClient(final HiveClient client) throws HiveServerException, TException {
        return client.fetchAll();
      }
    });
  }

  @Override
  public <T> T execute(final HiveClientCallback<T> callback) {
    try {
      return callback.doWithClient(client);
    } catch (final HiveServerException e) {
      throw exceptionTranslator.translate(e);
    } catch (final TException e) {
      throw exceptionTranslator.translate(e);
    }
  }
}
