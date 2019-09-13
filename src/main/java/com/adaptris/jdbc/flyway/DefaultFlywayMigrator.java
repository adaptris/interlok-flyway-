package com.adaptris.jdbc.flyway;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.sql.DataSource;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.BooleanUtils;
import org.flywaydb.core.Flyway;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.util.Args;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

/**
 * Perform a database migration using flyway
 * 
 * @config flyway-jdbc-migrator
 *
 */
@ComponentProfile(summary = "Perform a database migration using flyway", since = "3.9.2")
@XStreamAlias("flyway-jdbc-migrator")
public class DefaultFlywayMigrator implements FlywayMigrator {

  @XStreamImplicit(itemFieldName = "flyway-location")
  @NotNull
  @Valid
  @AutoPopulated
  private List<String> flywayLocations;

  @AdvancedConfig
  @InputFieldDefault(value = "false")
  private Boolean baseline;

  public DefaultFlywayMigrator() {
    setFlywayLocations(new ArrayList<>());
  }

  @Override
  public void migrate(DataSource source) throws Exception {
    Flyway flyway = Flyway.configure().dataSource(source).locations(getFlywayLocations().toArray(new String[0])).load();
    if (baseline()) {
      flyway.baseline();
    }
    flyway.migrate();
  }

  public List<String> getFlywayLocations() {
    return flywayLocations;
  }

  /**
   * List of locations to scan recursively for migrations.
   *
   * <p>
   * Locations can be prefixed with classpath or filesytem. which means that {@code filesystem:./sql/}
   * if the files are on the filesystem or {@code classpath:migration/mysql} if they are on the
   * classpath.
   * </p>
   *
   * @param flywayLocations the locations.
   */
  public void setFlywayLocations(List<String> flywayLocations) {
    this.flywayLocations = Args.notNull(flywayLocations, "flywayLocations");
  }

  public DefaultFlywayMigrator withFlywayLocations(List<String> locations) {
    setFlywayLocations(locations);
    return this;
  }

  public DefaultFlywayMigrator withFlywayLocations(String... locations) {
    return withFlywayLocations(new ArrayList<>(Arrays.asList(locations)));
  }

  public Boolean getBaseline() {
    return baseline;
  }

  /**
   * Whether to automatically call baseline when migrate is executed against a non-empty schema with
   * no schema history table.
   *
   * @param baseline true to baseline, default is false if not explicitly configured.
   */
  public void setBaseline(Boolean baseline) {
    this.baseline = baseline;
  }

  public DefaultFlywayMigrator withBaseline(Boolean b) {
    setBaseline(b);
    return this;
  }

  private boolean baseline() {
    return BooleanUtils.toBooleanDefaultIfNull(getBaseline(), false);
  }
}
