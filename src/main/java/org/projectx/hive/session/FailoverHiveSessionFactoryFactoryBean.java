package org.projectx.hive.session;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.projectx.hive.Host;
import org.projectx.hive.connection.HiveConnectionFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * {@link FactoryBean} implementation which produces a configured
 * {@link FailoverHiveSessionFactory}.
 * <p>
 * This implementation adds an ability to provide the hive host list as
 * comma-delimited string host:port pairs which are parsed to a {@link Host}.
 * <p>
 * A poor man's load balancing between failover hive servers can be achieved by
 * setting the {@link #setShuffleHostListOnStartup(boolean)} which performs
 * shuffle on the host list order (this causes initial and failover candidates
 * to be randomized across a cluster of hive thrift servers)
 * 
 * @see FailoverHiveSessionFactory
 * @see FailoverHiveSession
 * @author erez
 * 
 */
public class FailoverHiveSessionFactoryFactoryBean implements InitializingBean,
    FactoryBean<FailoverHiveSessionFactory> {

  private static final String PORT_DELIMITER = ":";
  private FailoverHiveSessionFactory failoverHiveSessionFactory;
  private List<Host> hosts = new LinkedList<Host>();
  private int timeout = 120000;
  private String connectionTestQuery = "select 1 from dual";
  private int connectionTestQueryTimeout = 30000;
  private int numAttempts = 1;
  private boolean shuffleHostListOnStartup = false;
  private final HiveConnectionFactory hiveConnectionFactory;

  public FailoverHiveSessionFactoryFactoryBean(final HiveConnectionFactory hiveConnectionFactory) {
    Assert.notNull(hiveConnectionFactory, "hiveConnectionFactory cannot be null");
    this.hiveConnectionFactory = hiveConnectionFactory;
  }

  @Override
  public FailoverHiveSessionFactory getObject() throws Exception {
    return failoverHiveSessionFactory;
  }

  @Override
  public Class<FailoverHiveSession> getObjectType() {
    return FailoverHiveSession.class;
  }

  @Override
  public boolean isSingleton() {
    return true;
  }

  public void setHosts(final List<Host> hosts) {
    this.hosts = hosts;
  }

  public void setHostsString(final String hostsString) {
    Assert.hasText(hostsString, "hostsString cannot be empty");
    final List<String> hostsStrings = Arrays.asList(StringUtils.commaDelimitedListToStringArray(hostsString));
    final List<Host> hostsList = new LinkedList<Host>();
    for (final String hostAndPort : hostsStrings) {
      Assert.isTrue(hostAndPort.contains(":"),
          "hosts string must contain the ':' delimiter to denote the host and port");
      final String[] hostParts = StringUtils.delimitedListToStringArray(hostAndPort, PORT_DELIMITER);
      hostsList.add(new Host(hostParts[0], Integer.valueOf(hostParts[1])));
    }
    hosts.addAll(hostsList);
  }

  public void setTimeout(final int timeout) {
    this.timeout = timeout;
  }

  public void setConnectionTestQuery(final String connectionTestQuery) {
    this.connectionTestQuery = connectionTestQuery;
  }

  public void setConnectionTestQueryTimeout(final int connectionTestQueryTimeout) {
    this.connectionTestQueryTimeout = connectionTestQueryTimeout;
  }

  public void setNumAttempts(final int numAttempts) {
    this.numAttempts = numAttempts;
  }

  public void setShuffleHostListOnStartup(final boolean shuffleHostListOnStartup) {
    this.shuffleHostListOnStartup = shuffleHostListOnStartup;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    if (shuffleHostListOnStartup) {
      Collections.shuffle(hosts);
    }
    if (hosts.isEmpty()) {
      throw new IllegalArgumentException(
          ("No hosts provided use hosts or hostsString properties to specificy the hive hosts"));
    }

    failoverHiveSessionFactory = new FailoverHiveSessionFactory(hiveConnectionFactory, hosts,
        timeout, connectionTestQuery, connectionTestQueryTimeout, numAttempts);
  }
}
