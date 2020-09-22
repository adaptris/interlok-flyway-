package com.adaptris.jdbc.flyway;

import javax.validation.Valid;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.CoreException;
import com.adaptris.core.jdbc.AdvancedJdbcPooledConnection;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Extension of {@link AdvancedJdbcPooledConnection} which will run flyway migration on init.
 *
 * @config flyway-jdbc-connection
 */
@XStreamAlias("flyway-jdbc-connection")
@ComponentProfile(summary = "Extension of AdvancedJdbcPooledConnection which will run flyway migration on init.",
    tag = "connections,jdbc,flyway",
    since = "3.9.0")
@DisplayOrder(order = {"username", "password", "driverImp", "connectUrl",
    "connectionPoolProperties", "connectionProperties", "flyway"})
@NoArgsConstructor
public class FlywayJdbcConnection extends AdvancedJdbcPooledConnection {

  private transient boolean warningLogged = false;

  @Valid
  @InputFieldDefault(value = "does nothing")
  @Getter
  @Setter
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

  public FlywayJdbcConnection withFlyway(DefaultFlywayMigrator flyway) {
    setFlyway(flyway);
    return this;
  }

  protected FlywayMigrator migrator() {
    return FlywayMigrator.defaultIfNull(getFlyway());
  }
}
