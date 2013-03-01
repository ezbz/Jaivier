package org.projectx.hive.session;

import java.util.List;

import org.projectx.hive.HiveDataAccessException;
import org.projectx.hive.Host;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LoggingHiveSessionEventHandler extends AbstractHiveSessionEventHandler {
  private static final Logger log = LoggerFactory.getLogger(LoggingHiveSessionEventHandler.class);

  @Override
  public void connectAttempt(final Host host, final int attempt, final int numAttemptsPerHost, final String connectionTestQuery) {
    log.debug("Executing connection test on host [{}], attempt [{}/{}], test query [{}]", new Object[] { host, attempt, numAttemptsPerHost,
                                                                                                        connectionTestQuery });
  }

  @Override
  public void connectFailure(final Host host, final int attempt, final int numAttemptsPerHost, final HiveDataAccessException e) {
    log.warn(String.format("Encountered a HiveDataAccessException while connecting to host [%s] attempt [%s/%s]", host, attempt, numAttemptsPerHost),
        e);
  }

  @Override
  public void connectOptionsExhausted(final List<Host> hosts, final int numAttemptsPerHost, final String message) {
    log.error(message);
  }

  @Override
  public void closeException(final HiveDataAccessException e) {
    log.debug("Error while trying to close a failed connection");
  }

}
