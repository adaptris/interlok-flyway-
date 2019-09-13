package com.adaptris.jdbc.flyway;

import javax.validation.Valid;
import org.apache.commons.lang3.ObjectUtils;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.CoreException;
import com.adaptris.core.jdbc.PluggableJdbcPooledConnection;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Extension of {@link PluggableJdbcPooledConnection} which will run flyway migration on init.
 * 
 * @config flyway-pluggable-jdbc-connection
 * 
 */
@XStreamAlias("flyway-pluggable-jdbc-connection")
@ComponentProfile(summary = "Extension of PluggableJdbcPooledConnection which will run flyway migration on init.",
    tag = "connections,jdbc,flyway",
    since = "3.9.2")
public class FlywayPluggableConnection extends PluggableJdbcPooledConnection {

  @Valid
  @InputFieldDefault(value = "does nothing")
  private FlywayMigrator flyway;

  public FlywayPluggableConnection() {
  }

  @Override
  protected void initialiseDatabaseConnection() throws CoreException {
    try {
      super.initialiseDatabaseConnection();
      migrator().migrate(asDataSource());
    } catch (Exception e) {
      throw ExceptionHelper.wrapCoreException(e);
    }
  }

  public FlywayMigrator getFlyway() {
    return flyway;
  }

  /**
   * Configure the migrator.
   * 
   * @param flyway the flyway migrator.
   */
  public void setFlyway(FlywayMigrator flyway) {
    this.flyway = flyway;
  }

  public FlywayPluggableConnection withFlyway(FlywayMigrator flyway) {
    setFlyway(flyway);
    return this;
  }

  protected FlywayMigrator migrator() {
    return ObjectUtils.defaultIfNull(getFlyway(), (ds) -> {
    });
  }
}
