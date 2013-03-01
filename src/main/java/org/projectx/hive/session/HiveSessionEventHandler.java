package org.projectx.hive.session;

import java.util.List;

import org.projectx.hive.HiveDataAccessException;
import org.projectx.hive.Host;


/**
 * An event handler for a hive connect attempt
 * @author erez
 *
 */
public interface HiveSessionEventHandler {

  void connectAttempt(Host host, int attempt, int numAttemptsPerHost, String connectionTestQuery);

  void connectFailure(Host host, int attempt, int numAttemptsPerHost, HiveDataAccessException e);

  void connectSuccess(Host host, int attempt, int numAttemptsPerHost);

  void connectOptionsExhausted(List<Host> hosts, int numAttemptsPerHost, String message);

  void closeException(HiveDataAccessException e);

}
