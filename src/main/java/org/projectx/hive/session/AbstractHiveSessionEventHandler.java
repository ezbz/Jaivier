package org.projectx.hive.session;

import java.util.List;

import org.projectx.hive.HiveDataAccessException;
import org.projectx.hive.Host;


public abstract class AbstractHiveSessionEventHandler implements HiveSessionEventHandler {

  @Override
  public void connectAttempt(final Host host, final int attempt, final int numAttemptsPerHost, final String connectionTestQuery) {
  }

  @Override
  public void connectFailure(final Host host, final int attempt, final int numAttemptsPerHost, final HiveDataAccessException e) {
  }

  @Override
  public void connectSuccess(final Host host, final int attempt, final int numAttemptsPerHost) {
  }

  @Override
  public void connectOptionsExhausted(final List<Host> hosts, final int numAttemptsPerHost, final String message) {
  }

  @Override
  public void closeException(final HiveDataAccessException e) {
  }

}
