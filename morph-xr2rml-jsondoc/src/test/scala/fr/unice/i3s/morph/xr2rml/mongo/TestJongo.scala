package fr.unice.i3s.morph.xr2rml.mongo

import scala.collection.JavaConversions.seqAsJavaList

import org.jongo.Jongo
import org.jongo.MongoCollection
import org.jongo.MongoCursor
import org.junit.Test

import com.mongodb.DB
import com.mongodb.MongoClient
import com.mongodb.MongoCredential
import com.mongodb.ServerAddress

class TestJongo {

    @Test def Test() {

        val user: String = "user"
        val pwd: String = "user"
        val database: String = "test"

        val servAdr = new ServerAddress("localhost", 27017)

        val dbCnx: DB =
            if (user.isEmpty() && pwd.isEmpty()) {
                new MongoClient(servAdr).getDB(database)
            } else {
                val cred = MongoCredential.createMongoCRCredential(user, database, pwd.toCharArray())
                new MongoClient(servAdr, seqAsJavaList(List(cred))).getDB(database)
            }

        val jongoCnx: Jongo = new Jongo(dbCnx)

        val collec: MongoCollection = jongoCnx.getCollection("testData")
        val handler: JongoResultHandler = new JongoResultHandler
        val all: MongoCursor[String] = collec.find("{ 'a': { $exists: true} }").map(handler)

        while (all.hasNext())
            println(all.next())
    }
}