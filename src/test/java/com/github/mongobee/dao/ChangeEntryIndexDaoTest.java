package com.github.mongobee.dao;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.bson.Document;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.github.mongobee.changeset.ChangeEntry;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.testcontainers.containers.MongoDBContainer;

/**
 * @author lstolowski
 * @since 10.12.14
 */
public class ChangeEntryIndexDaoTest {

  @ClassRule
  public static MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:7.0.16");

  private static final String TEST_SERVER = "testServer";
  private static final String DB_NAME = "mongobeetest";
  private static final String CHANGEID_AUTHOR_INDEX_NAME = "changeId_1_author_1";
  private static final String CHANGELOG_COLLECTION_NAME = "dbchangelog";

  private ChangeEntryIndexDao dao = new ChangeEntryIndexDao(CHANGELOG_COLLECTION_NAME);

  private static MongoClient testMongoClient;

  @BeforeClass
  public static void setUp() {
    String connectionString = mongoDBContainer.getReplicaSetUrl(DB_NAME);
    testMongoClient = new MongoClient(new MongoClientURI(connectionString));
  }

  @AfterClass
  public static void tearDown() {
    if (testMongoClient != null) {
      testMongoClient.close();
    }
  }

  private static MongoDatabase createTestDatabase() {
    return testMongoClient.getDatabase(DB_NAME);
  }

  @Test
  public void shouldCreateRequiredUniqueIndex() {
    // given
    MongoClient mongo = mock(MongoClient.class);
    MongoDatabase db = createTestDatabase();
    when(mongo.getDatabase(Mockito.anyString())).thenReturn(db);

    // when
    dao.createRequiredUniqueIndex(db.getCollection(CHANGELOG_COLLECTION_NAME));

    // then
    Document createdIndex = findIndex(db, CHANGEID_AUTHOR_INDEX_NAME);
    assertNotNull(createdIndex);
    assertTrue(dao.isUnique(createdIndex));
  }

  @Test
  @Ignore("Fongo has not implemented dropIndex for MongoCollection object (issue with mongo driver 3.x)")
  public void shouldDropWrongIndex() {
    // init
    MongoClient mongo = mock(MongoClient.class);
    MongoDatabase db = createTestDatabase();
    when(mongo.getDatabase(Mockito.anyString())).thenReturn(db);

    MongoCollection<Document> collection = db.getCollection(CHANGELOG_COLLECTION_NAME);
    collection.createIndex(new Document()
        .append(ChangeEntry.KEY_CHANGEID, 1)
        .append(ChangeEntry.KEY_AUTHOR, 1));
    Document index = new Document("name", CHANGEID_AUTHOR_INDEX_NAME);

    // given
    Document createdIndex = findIndex(db, CHANGEID_AUTHOR_INDEX_NAME);
    assertNotNull(createdIndex);
    assertFalse(dao.isUnique(createdIndex));

    // when
    dao.dropIndex(db.getCollection(CHANGELOG_COLLECTION_NAME), index);

    // then
    assertNull(findIndex(db, CHANGEID_AUTHOR_INDEX_NAME));
  }

  private Document findIndex(MongoDatabase db, String indexName) {

    for (MongoCursor<Document> iterator = db.getCollection(CHANGELOG_COLLECTION_NAME).listIndexes().iterator(); iterator.hasNext(); ) {
      Document index = iterator.next();
      String name = (String) index.get("name");
      if (indexName.equals(name)) {
        return index;
      }
    }
    return null;
  }

}
