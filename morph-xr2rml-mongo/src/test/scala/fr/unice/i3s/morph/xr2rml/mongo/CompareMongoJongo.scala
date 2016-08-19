package fr.unice.i3s.morph.xr2rml.mongo

import scala.collection.JavaConversions.asScalaIterator
import scala.collection.JavaConversions.seqAsJavaList

import org.bson.Document
import org.bson.conversions.Bson
import org.jongo.Jongo
import org.junit.Test

import com.mongodb.Block
import com.mongodb.MongoClient
import com.mongodb.MongoCredential
import com.mongodb.ServerAddress
import com.mongodb.client.model.Filters.{ and => And }
import com.mongodb.client.model.Filters.{ eq => Eq }
import com.mongodb.client.model.Filters.exists
import com.mongodb.client.model.Filters.{ ne => Ne }
import com.mongodb.client.model.Filters.where

class CompareMongoJongo {

    var start: Long = 0
    var end: Long = 0
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

    private def execMongo(queryFilter: Bson, display: Boolean): Long = {
        val start = System.currentTimeMillis
        mongoCtx.getCollection(collecName).find(queryFilter).forEach(processBlock)
        val end = System.currentTimeMillis
        val time = end - start
        if (display)
            println("Process Mongo: " + time + "ms")
        time
    }

    private def execJongo(queryStr: String, display: Boolean): Long = {
        val start = System.currentTimeMillis
        var results: org.jongo.MongoCursor[String] = jongoCtx.getCollection(collecName).find(queryStr).map[String](jongoHandler)
        results.toList
        val end = System.currentTimeMillis
        val time = end - start
        if (display)
            println("Process Jongo: " + time + "ms")
        time
    }

    @Test def testQ1() {

        println("------------- testQ1 -------------")
        var sumq1Mongo: Long = 0
        var sumq2Mongo: Long = 0
        var sumq1Jongo: Long = 0
        var sumq2Jongo: Long = 0
        val nbIterations = 10

        // Equivalent queries as string for Jongo and Java filter for Mongo
        val q1Str = "{$where:'this.codeTaxon!=this.codeReference', 'codeTaxon': {$eq: '60587'}, 'codeReference': {$exists: true, $ne: null}}"
        val q1Filter = And(
            where("this.codeTaxon!=this.codeReference"),
            Eq("codeTaxon", "60587"),
            exists("codeReference", true), Ne("codeReference", null)
        )

        val q2Str = "{$where:'this.codeTaxon!=this.codeReference', 'codeTaxon': {$exists: true, $ne: null}, 'codeReference': {$exists: true, $ne: null}}"
        val q2Filter = And(
            where("this.codeTaxon!=this.codeReference"),
            exists("codeTaxon", true), Ne("codeTaxon", null),
            exists("codeReference", true), Ne("codeReference", null)
        )

        for (iter <- 1 to nbIterations) {
            sumq1Mongo += execMongo(q1Filter, true)
            sumq2Mongo += execMongo(q2Filter, true)
        }
        for (iter <- 1 to nbIterations) {
            sumq1Jongo += execJongo(q1Str, true)
            sumq2Jongo += execJongo(q2Str, true)
        }

        println("Average q1 Mongo: " + sumq1Mongo / nbIterations + " ms")
        println("Average q2 Mongo: " + sumq2Mongo / nbIterations + " ms")
        println("Average q1 Jongo: " + sumq1Jongo / nbIterations + " ms")
        println("Average q2 Jongo: " + sumq2Jongo / nbIterations + " ms")
    }

    @Test def testQ2() {

        println("------------- testQ2 -------------")
        var sumq1Mongo: Long = 0
        var sumq1Jongo: Long = 0
        val nbIterations = 10

        // Equivalent queries as string for Jongo and Java filter for Mongo
        val q1Str = "{$where:'this.codeTaxon==this.codeReference', 'codeParent': {$eq: '186212'}, 'codeTaxon': {$exists: true, $ne: null}}"
        val q1Filter = And(
            where("this.codeTaxon==this.codeReference"),
            Eq("codeParent", "186212"),
            exists("codeTaxon", true), Ne("codeTaxon", null)
        )

        val qFilterDiversion = And(
            where("this.codeTaxon==this.codeReference"),
            exists("codeTaxon", true), Ne("codeTaxon", null),
            Ne("nc", ""), Ne("nc", null)
        )

        for (iter <- 1 to nbIterations) {
            sumq1Mongo += execMongo(q1Filter, true)
            execMongo(qFilterDiversion, false)
            sumq1Jongo += execJongo(q1Str, true)
            execMongo(qFilterDiversion, false)
        }

        println("Average q1 Mongo: " + sumq1Mongo / nbIterations + " ms")
        println("Average q1 Jongo: " + sumq1Jongo / nbIterations + " ms")
    }

    @Test def testQ3() {

        println("------------- testQ3 -------------")
        var sumq3Mongo: Long = 0
        var sumq3Jongo: Long = 0
        val nbIterations = 10

        // Equivalent queries as string for Jongo and Java filter for Mongo
        val q3Str = "{$where:'this.codeTaxon==this.codeReference','fr':{$ne:''},'fr':{$ne:null}, 'codeTaxon': {$exists: true, $ne: null}}"
        val q3Filter = And(
            where("this.codeTaxon==this.codeReference"),
            exists("codeTaxon", true), Ne("codeTaxon", null),
            Ne("fr", ""), Ne("fr", null)
        )
        val qFilterDiversion = And(
            where("this.codeTaxon==this.codeReference"),
            exists("codeTaxon", true), Ne("codeTaxon", null),
            Ne("nc", ""), Ne("nc", null)
        )

        for (iter <- 1 to nbIterations) {
            sumq3Mongo += execMongo(q3Filter, true)
            //execMongo(qFilterDiversion, false)
            sumq3Jongo += execJongo(q3Str, true)
            //execMongo(qFilterDiversion, false)
        }

        println("Average q1 Mongo: " + sumq3Mongo / nbIterations + " ms")
        println("Average q1 Jongo: " + sumq3Jongo / nbIterations + " ms")
    }
}
