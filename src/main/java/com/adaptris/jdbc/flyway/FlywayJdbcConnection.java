package com.adaptris.jdbc.flyway;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.CoreException;
import com.adaptris.core.jdbc.AdvancedJdbcPooledConnection;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import org.flywaydb.core.Flyway;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Extension of JDBC connection which will run flyway migration on init.
 *
 */
@XStreamAlias("flyway-jdbc-connection")
@ComponentProfile(summary = "Extension of JDBC connection which will run flyway migration on init.", tag = "connections,jdbc,flyway")
public class FlywayJdbcConnection extends AdvancedJdbcPooledConnection {

  @XStreamImplicit(itemFieldName = "flyway-location")
  private List<String> flywayLocations = new ArrayList<>();

  @AdvancedConfig
  @InputFieldDefault(value = "false")
  private Boolean baseline;

  @Override
  protected void initialiseDatabaseConnection() throws CoreException {
    super.initialiseDatabaseConnection();
    migrate();
  }

  private void migrate() throws CoreException {
    try {
      Flyway flyway = Flyway.configure()
          .dataSource(asDataSource())
          .locations(getFlywayLocations().toArray(new String[]{}))
          .load();
      if(baseline()){
        flyway.baseline();
      }
      flyway.migrate();
    } catch (SQLException e) {
      ExceptionHelper.rethrowCoreException(e);
    }
  }

  public List<String> getFlywayLocations() {
    return flywayLocations;
  }

  /**
   * List of locations to scan recursively for migrations.
   *
   * <p>
   *   Locations can be prefixed with classpath or filesytem.
   * </p>
   *
   * <p>
   *   Example: filesystem:./sql/
   * </p>
   *
   * @param flywayLocations
   */
  public void setFlywayLocations(List<String> flywayLocations) {
    this.flywayLocations = Args.notNull(flywayLocations, "flywayLocations");
  }

  public Boolean getBaseline() {
    return baseline;
  }

  /**
   *  Whether to automatically call baseline when migrate is executed against a non-empty
   *  schema with no schema history table.
   *
    * @param baseline
   */
  public void setBaseline(Boolean baseline) {
    this.baseline = Args.notNull(baseline, "baseline");
  }

  boolean baseline(){
    return getBaseline() != null ? getBaseline() : false;
  }
}
