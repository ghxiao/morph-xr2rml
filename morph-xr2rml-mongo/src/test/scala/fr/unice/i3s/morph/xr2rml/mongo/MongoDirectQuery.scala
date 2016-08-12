package fr.unice.i3s.morph.xr2rml.mongo

import scala.collection.JavaConversions.asScalaIterator
import scala.collection.JavaConversions.seqAsJavaList

import org.bson.Document
import org.jongo.Jongo
import org.junit.Test

import com.mongodb.Block
import com.mongodb.MongoClient
import com.mongodb.MongoCredential
import com.mongodb.ServerAddress
import com.mongodb.client.model.Filters.{ and => And }
import com.mongodb.client.model.Filters.exists
import com.mongodb.client.model.Filters.{ ne => Ne }
import com.mongodb.client.model.Filters.where

class MongoDirectQuery {

    var start: Long = 0
    val userName = "user"
    val dbName = "test"
    val userPwd = "user"
    val collecName = "taxrefv8"

    val creds = MongoCredential.createCredential(userName, dbName, userPwd.toCharArray())
    val dbAdr = new ServerAddress("localhost", 27017)
    val mongoClient: MongoClient = new MongoClient(List(dbAdr), List(creds))

    // Define the MongoDB context
    val mongoCtx = mongoClient.getDatabase(dbName)
    val processBlock: Block[Document] = new Block[Document]() {
        @Override
        def apply(document: Document) {
            // Materialize each document as a JSON string
            val json = document.toJson
        }
    }

    // Define the Jongo context
    val jongoHandler: JongoResultHandler = new JongoResultHandler
    val jongoCtx = new Jongo(mongoClient.getDB(dbName))

    @Test def testQuery() {

        // Define equivalent query string for Jongo and query Java filter for Mongo
        var queryStr = "{$where:'this.codeTaxon!=this.codeReference', 'codeTaxon': {$exists: true, $ne: null}, 'codeReference': {$exists: true, $ne: null}}"
        var queryFilter = And(
            where("this.codeTaxon!=this.codeReference"),
            exists("codeTaxon", true), Ne("codeTaxon", null),
            exists("codeReference", true), Ne("codeReference", null)
        )

        // Direct query with MongoDB driver
        start = System.currentTimeMillis
        mongoCtx.getCollection(collecName).find(queryFilter).forEach(processBlock)
        println("Process Mongo: " + (System.currentTimeMillis - start) + "ms")

        // Query with Jongo
        start = System.currentTimeMillis
        var results: org.jongo.MongoCursor[String] = jongoCtx.getCollection(collecName).find(queryStr).map[String](jongoHandler)
        results.toList
        println("Process Jongo: " + (System.currentTimeMillis - start) + "ms")
    }
}
