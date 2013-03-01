package org.projectx.hive;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.springframework.util.Assert;

/**
 * A representation of a hive-thrift server on a give {@link #getHost() host}
 * and {@link #getPort() port}
 * 
 * @author erez
 * 
 */
public class Host {

  private final String host;
  private final Integer port;

  public Host(final String host, final Integer port) {
    Assert.hasText(host, "host cannot be empty");
    Assert.notNull(port, "port cannot be null");
    this.host = host;
    this.port = port;
  }

  public String getHost() {
    return host;
  }

  public Integer getPort() {
    return port;
  }

  @Override
  public boolean equals(final Object other) {

    if (other == this) {
      return true;
    }

    if (!(other instanceof Host)) {
      return false;
    }

    final Host o = (Host) other;
    final EqualsBuilder eb = new EqualsBuilder();
    eb.append(getHost(), o.getHost());
    eb.append(getPort(), o.getPort());
    return eb.isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(getHost()).append(getPort()).toHashCode();
  }

  @Override
  public String toString() {
    final ToStringBuilder tsb = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
    tsb.append(getHost());
    tsb.append(getPort());
    return tsb.toString();
  }

}
