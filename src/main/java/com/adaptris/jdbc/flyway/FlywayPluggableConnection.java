package com.adaptris.jdbc.flyway;

import javax.validation.Valid;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.CoreException;
import com.adaptris.core.jdbc.PluggableJdbcPooledConnection;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Extension of {@link PluggableJdbcPooledConnection} which will run flyway migration on init.
 *
 * @config flyway-pluggable-jdbc-connection
 *
 */
@XStreamAlias("flyway-pluggable-jdbc-connection")
@ComponentProfile(summary = "Extension of PluggableJdbcPooledConnection which will run flyway migration on init.", tag = "connections,jdbc,flyway", since = "3.9.2")
@DisplayOrder(order = { "username", "password", "driverImp", "connectUrl", "flyway", "builder", "poolProperties", "connectionProperties" })
@NoArgsConstructor
public class FlywayPluggableConnection extends PluggableJdbcPooledConnection {

  /**
   * Configure the migrator.
   */
  @Valid
  @InputFieldDefault(value = "does nothing")
  @Setter
  @Getter
  private FlywayMigrator flyway;

  @Override
  protected void initialiseDatabaseConnection() throws CoreException {
    try {
      super.initialiseDatabaseConnection();
      migrator().migrate(asDataSource());
    } catch (Exception e) {
      throw ExceptionHelper.wrapCoreException(e);
    }
  }

  public FlywayPluggableConnection withFlyway(FlywayMigrator flyway) {
    setFlyway(flyway);
    return this;
  }

  protected FlywayMigrator migrator() {
    return FlywayMigrator.defaultIfNull(getFlyway());
  }

}
