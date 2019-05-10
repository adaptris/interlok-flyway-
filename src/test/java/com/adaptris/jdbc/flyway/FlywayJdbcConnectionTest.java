package com.adaptris.jdbc.flyway;

import com.adaptris.core.jdbc.DatabaseConnectionCase;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.util.TimeInterval;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class FlywayJdbcConnectionTest extends DatabaseConnectionCase<FlywayJdbcConnection> {

  public FlywayJdbcConnectionTest(String arg0) {
    super(arg0);
  }

  @Test
  public void testGetFlywayLocations(){
    FlywayJdbcConnection connection = new FlywayJdbcConnection();
    connection.setFlywayLocations(Collections.singletonList("classpath:migration"));
    assertEquals(1, connection.getFlywayLocations().size());
    assertEquals("classpath:migration", connection.getFlywayLocations().get(0));
  }

  public void testGetBaseline() {
    FlywayJdbcConnection connection = new FlywayJdbcConnection();
    assertNull(connection.getBaseline());
    assertFalse(connection.baseline());
    connection.setBaseline(true);
    assertTrue(connection.getBaseline());
    assertTrue(connection.baseline());
  }

  public void testConnectionWhenInitialisedFullMigration() throws Exception {
    FlywayJdbcConnection con = configure(createConnection(), initialiseFlywayDatabase());
    con.setBaseline(false);
    con.setFlywayLocations(Collections.singletonList("classpath:migration/full"));
    LifecycleHelper.init(con);
    con.connect();
  }

  @Override
  protected FlywayJdbcConnection createConnection() {
    return new FlywayJdbcConnection();
  }

  @Override
  protected FlywayJdbcConnection configure(FlywayJdbcConnection flywayJdbcConnection) throws Exception {
    String url = initialiseDatabase();
    return configure(flywayJdbcConnection, url);
  }

  private FlywayJdbcConnection configure(FlywayJdbcConnection flywayJdbcConnection, String url) throws Exception {
    flywayJdbcConnection.setConnectUrl(url);
    flywayJdbcConnection.setDriverImp(DRIVER_IMP);
    flywayJdbcConnection.setTestStatement(DEFAULT_TEST_STATEMENT);
    flywayJdbcConnection.setDebugMode(true);
    flywayJdbcConnection.setConnectionAttempts(1);
    flywayJdbcConnection.setConnectionRetryInterval(new TimeInterval(10L, TimeUnit.MILLISECONDS.name()));
    flywayJdbcConnection.setAlwaysValidateConnection(false);
    flywayJdbcConnection.setBaseline(true);
    flywayJdbcConnection.setFlywayLocations(Collections.singletonList("classpath:migration/partial"));
    return flywayJdbcConnection;
  }

  protected String initialiseFlywayDatabase() throws Exception {
    return "jdbc:derby:memory:" + nameGen.safeUUID() + ";create=true";
  }
}