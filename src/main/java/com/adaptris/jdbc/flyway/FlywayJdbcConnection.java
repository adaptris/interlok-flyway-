package com.adaptris.jdbc.flyway;

import java.util.List;
import javax.validation.Valid;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.Removal;
import com.adaptris.core.CoreException;
import com.adaptris.core.jdbc.AdvancedJdbcPooledConnection;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.LoggingHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

/**
 * Extension of {@link AdvancedJdbcPooledConnection} which will run flyway migration on init.
 * 
 * @config flyway-jdbc-connection
 */
@XStreamAlias("flyway-jdbc-connection")
@ComponentProfile(summary = "Extension of AdvancedJdbcPooledConnection which will run flyway migration on init.",
    tag = "connections,jdbc,flyway",
    since = "3.9.0")
public class FlywayJdbcConnection extends AdvancedJdbcPooledConnection {

  private transient boolean warningLogged = false;

  @Valid
  @XStreamImplicit(itemFieldName = "flyway-location")
  @Deprecated
  @Removal(version = "3.11.0", message = "use flyway-migrator instead")
  private List<String> flywayLocations;

  @AdvancedConfig
  @InputFieldDefault(value = "false")
  @Deprecated
  @Removal(version = "3.11.0", message = "use flyway-migrator instead")
  private Boolean baseline;

  @Valid
  @InputFieldDefault(value = "does nothing")
  private FlywayMigrator flyway;

  public FlywayJdbcConnection() {
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

  @Deprecated
  @Removal(version = "3.10.0", message = "use flyway-migrator instead")
  public List<String> getFlywayLocations() {
    return flywayLocations;
  }

  /**
   * List of locations to scan recursively for migrations.
   *
   * <p>
   * Locations can be prefixed with classpath or filesytem.
   * </p>
   *
   * <p>
   * Example: filesystem:./sql/
   * </p>
   *
   * @param flywayLocations the locations.
   * @deprecated since 3.9.2 use a flyway-migrator instead.
   */
  @Deprecated
  @Removal(version = "3.11.0", message = "use flyway-migrator instead")
  public void setFlywayLocations(List<String> flywayLocations) {
    this.flywayLocations = flywayLocations;
  }

  @Deprecated
  @Removal(version = "3.11.0", message = "use flyway-migrator instead")
  public Boolean getBaseline() {
    return baseline;
  }

  /**
   * Whether to automatically call baseline when migrate is executed against a non-empty schema with
   * no schema history table.
   *
   * @param baseline true to baseline; default is false if not explicitly configured.
   * @deprecated since 3.9.2 use a flyway-migrator instead.
   */
  @Deprecated
  @Removal(version = "3.11.0", message = "use flyway-migrator instead")
  public void setBaseline(Boolean baseline) {
    this.baseline = baseline;
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
    this.flyway = Args.notNull(flyway, "flyway");
  }

  public FlywayJdbcConnection withFlyway(DefaultFlywayMigrator flyway) {
    setFlyway(flyway);
    return this;
  }

  protected FlywayMigrator migrator() {
    if (CollectionUtils.isNotEmpty(getFlywayLocations())) {
      LoggingHelper.logWarning(warningLogged, () -> {
        warningLogged = true;
      }, "flyway-locations configured in {}; use flyway-migrator instead", LoggingHelper.friendlyName(this));
      return new DefaultFlywayMigrator().withFlywayLocations(getFlywayLocations()).withBaseline(getBaseline());
    }
    return ObjectUtils.defaultIfNull(getFlyway(), (ds) -> {
    });
  }
}
