package com.github.mongobee;

import com.github.mongobee.changeset.ChangeEntry;
import com.github.mongobee.dao.ChangeEntryDao;
import com.github.mongobee.dao.ChangeEntryIndexDao;
import com.github.mongobee.resources.EnvironmentMock;
import com.github.mongobee.test.changelogs.AnotherMongobeeTestResource;
import com.github.mongobee.test.profiles.def.UnProfiledChangeLog;
import com.github.mongobee.test.profiles.dev.ProfiledDevChangeLog;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.testcontainers.containers.MongoDBContainer;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.when;

/**
 * Tests for Spring profiles integration
 *
 * @author lstolowski
 * @since 2014-09-17
 */
@RunWith(MockitoJUnitRunner.class)
public class MongobeeProfileTest {

  @ClassRule
  public static MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:7.0.16");

  private static final String CHANGELOG_COLLECTION_NAME = "dbchangelog";
  public static final int CHANGELOG_COUNT = 13;

  @InjectMocks
  private Mongobee runner = new Mongobee();

  @Mock
  private ChangeEntryDao dao;

  @Mock
  private ChangeEntryIndexDao indexDao;

  private MongoDatabase fakeMongoDatabase;
  private MongoClient mongoClient;

  @Before
  public void init() throws Exception {
    ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("mongobeetest"));
    mongoClient = MongoClients.create(connectionString);
    fakeMongoDatabase = mongoClient.getDatabase("mongobeetest");

    when(dao.connectMongoDb(any(ConnectionString.class), anyString()))
        .thenReturn(fakeMongoDatabase);
    when(dao.getMongoDatabase()).thenReturn(fakeMongoDatabase);
    when(dao.acquireProcessLock()).thenReturn(true);
    doCallRealMethod().when(dao).save(any(ChangeEntry.class));
    doCallRealMethod().when(dao).setChangelogCollectionName(anyString());
    doCallRealMethod().when(dao).setIndexDao(any(ChangeEntryIndexDao.class));
    dao.setIndexDao(indexDao);
    dao.setChangelogCollectionName(CHANGELOG_COLLECTION_NAME);

    runner.setConnectionString(connectionString);
    runner.setDbName("mongobeetest");
    runner.setEnabled(true);
  } // TODO code duplication

  @Test
  public void shouldRunDevProfileAndNonAnnotated() throws Exception {
    // given
    runner.setSpringEnvironment(new EnvironmentMock("dev", "test"));
    runner.setChangeLogsScanPackage(ProfiledDevChangeLog.class.getPackage().getName());
    when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);

    // when
    runner.execute();

    // then
    long change1 = fakeMongoDatabase.getCollection(CHANGELOG_COLLECTION_NAME)
        .countDocuments(new Document()
            .append(ChangeEntry.KEY_CHANGEID, "Pdev1")
            .append(ChangeEntry.KEY_AUTHOR, "testuser"));
    assertEquals(1, change1);  //  no-@Profile  should not match

    long change2 = fakeMongoDatabase.getCollection(CHANGELOG_COLLECTION_NAME)
        .countDocuments(new Document()
            .append(ChangeEntry.KEY_CHANGEID, "Pdev4")
            .append(ChangeEntry.KEY_AUTHOR, "testuser"));
    assertEquals(1, change2);  //  @Profile("dev")  should not match

    long change3 = fakeMongoDatabase.getCollection(CHANGELOG_COLLECTION_NAME)
        .countDocuments(new Document()
            .append(ChangeEntry.KEY_CHANGEID, "Pdev3")
            .append(ChangeEntry.KEY_AUTHOR, "testuser"));
    assertEquals(0, change3);  //  @Profile("default")  should not match
  }

  @Test
  public void shouldRunUnprofiledChangeLog() throws Exception {
    // given
    runner.setSpringEnvironment(new EnvironmentMock("test"));
    runner.setChangeLogsScanPackage(UnProfiledChangeLog.class.getPackage().getName());
    when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);

    // when
    runner.execute();

    // then
    long change1 = fakeMongoDatabase.getCollection(CHANGELOG_COLLECTION_NAME)
        .countDocuments(new Document()
            .append(ChangeEntry.KEY_CHANGEID, "Pdev1")
            .append(ChangeEntry.KEY_AUTHOR, "testuser"));
    assertEquals(1, change1);

    long change2 = fakeMongoDatabase.getCollection(CHANGELOG_COLLECTION_NAME)
        .countDocuments(new Document()
            .append(ChangeEntry.KEY_CHANGEID, "Pdev2")
            .append(ChangeEntry.KEY_AUTHOR, "testuser"));
    assertEquals(1, change2);

    long change3 = fakeMongoDatabase.getCollection(CHANGELOG_COLLECTION_NAME)
        .countDocuments(new Document()
            .append(ChangeEntry.KEY_CHANGEID, "Pdev3")
            .append(ChangeEntry.KEY_AUTHOR, "testuser"));
    assertEquals(1, change3);  //  @Profile("dev")  should not match

    long change4 = fakeMongoDatabase.getCollection(CHANGELOG_COLLECTION_NAME)
        .countDocuments(new Document()
            .append(ChangeEntry.KEY_CHANGEID, "Pdev4")
            .append(ChangeEntry.KEY_AUTHOR, "testuser"));
    assertEquals(0, change4);  //  @Profile("pro")  should not match

    long change5 = fakeMongoDatabase.getCollection(CHANGELOG_COLLECTION_NAME)
        .countDocuments(new Document()
            .append(ChangeEntry.KEY_CHANGEID, "Pdev5")
            .append(ChangeEntry.KEY_AUTHOR, "testuser"));
    assertEquals(1, change5);  //  @Profile("!pro")  should match
  }

  @Test
  public void shouldNotRunAnyChangeSet() throws Exception {
    // given
    runner.setSpringEnvironment(new EnvironmentMock("foobar"));
    runner.setChangeLogsScanPackage(ProfiledDevChangeLog.class.getPackage().getName());

    // when
    runner.execute();

    // then
    long changes = fakeMongoDatabase.getCollection(CHANGELOG_COLLECTION_NAME).countDocuments(new Document());
    assertEquals(0, changes);
  }

  @Test
  public void shouldRunChangeSetsWhenNoEnv() throws Exception {
    // given
    runner.setSpringEnvironment(null);
    runner.setChangeLogsScanPackage(AnotherMongobeeTestResource.class.getPackage().getName());
    when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);

    // when
    runner.execute();

    // then
    long changes = fakeMongoDatabase.getCollection(CHANGELOG_COLLECTION_NAME).countDocuments(new Document());
    assertEquals(CHANGELOG_COUNT, changes);
  }

  @Test
  public void shouldRunChangeSetsWhenEmptyEnv() throws Exception {
    // given
    runner.setSpringEnvironment(new EnvironmentMock());
    runner.setChangeLogsScanPackage(AnotherMongobeeTestResource.class.getPackage().getName());
    when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);

    // when
    runner.execute();

    // then
    long changes = fakeMongoDatabase.getCollection(CHANGELOG_COLLECTION_NAME).countDocuments(new Document());
    assertEquals(CHANGELOG_COUNT, changes);
  }

  @Test
  public void shouldRunAllChangeSets() throws Exception {
    // given
    runner.setSpringEnvironment(new EnvironmentMock("dev"));
    runner.setChangeLogsScanPackage(AnotherMongobeeTestResource.class.getPackage().getName());
    when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);

    // when
    runner.execute();

    // then
    long changes = fakeMongoDatabase.getCollection(CHANGELOG_COLLECTION_NAME).countDocuments(new Document());
    assertEquals(CHANGELOG_COUNT, changes);
  }

  @After
  public void cleanUp() {
    runner.setMongoTemplate(null);
    fakeMongoDatabase.drop();
    if (mongoClient != null) {
      mongoClient.close();
    }
  }

}
