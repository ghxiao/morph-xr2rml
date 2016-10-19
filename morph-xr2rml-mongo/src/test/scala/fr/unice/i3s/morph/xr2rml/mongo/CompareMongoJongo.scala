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
    val collecName = "taxrefv9"

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

    /**
     * Execute a MongoDB query defined as a org.bson.conversions.Bons filter
     */
    private def execMongo(queryFilter: Bson, display: Boolean): Long = {
        val start = System.currentTimeMillis
        mongoCtx.getCollection(collecName).find(queryFilter).forEach(processBlock)
        val end = System.currentTimeMillis
        val time = end - start
        if (display)
            println("Process Mongo: " + time + "ms")
        time
    }

    /**
     * Execute a Jongo query given as a simply query string
     */
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

    /**
     * Executate Mongo and Jongo queries and calculate average and standard deviation
     */
    private def runMeasure(mongoQuery: Bson, jongoQuery: String, nbIterations: Int) {
        var sumMongo: Long = 0
        var sumJongo: Long = 0

        // Warm-up execution
        execMongo(mongoQuery, false)
        execJongo(jongoQuery, false)
        
        // Execution
        val measures = for (iter <- 1 to nbIterations) yield {
            val mongo = execMongo(mongoQuery, true)
            val jongo = execJongo(jongoQuery, true)
            sumMongo += mongo
            sumJongo += jongo
            (mongo, jongo)
        }

        var avgMongo = sumMongo / nbIterations
        val devMongo = Math.sqrt(measures.map(mes => Math.pow(mes._1 - avgMongo, 2)).sum / nbIterations)

        var avgJongo = sumJongo / nbIterations
        val devJongo = Math.sqrt(measures.map(mes => Math.pow(mes._2 - avgJongo, 2)).sum / nbIterations)

        println("Average Mongo: " + sumMongo / nbIterations + " ms. " + "Std deviation: " + devMongo)
        println("Average Jongo: " + sumJongo / nbIterations + " ms. " + "Std deviation: " + devJongo)
    }

    @Test def testQ0() {
        println("------------- testQ0 - 1 result -------------")
        val nbIterations = 50

        // Equivalent queries as string for Jongo and Java filter for Mongo
        val qStr = "{$where:'this.codeTaxon==this.codeReference', 'codeTaxon': {$eq: '60587', $exists: true, $ne: null}, 'codeReference': {$exists: true, $ne: null}}"
        val qFilter = And(
            where("this.codeTaxon==this.codeReference"),
            Eq("codeTaxon", "60587"),
            exists("codeTaxon", true), Ne("codeTaxon", null),
            exists("codeReference", true), Ne("codeReference", null)
        )

        runMeasure(qFilter, qStr, nbIterations)
    }

    @Test def testQ1() {
        println("------------- testQ1 - 164 results -------------")
        val nbIterations = 10

        // Equivalent queries as string for Jongo and Java filter for Mongo
        val qStr = "{$where:'this.codeTaxon!=this.codeReference', 'codeTaxon': {$exists: true, $ne: null}, 'codeReference': {$eq: '95372'}}"
        val qFilter = And(
            where("this.codeTaxon!=this.codeReference"),
            Eq("codeReference", "95372"),
            exists("codeTaxon", true), Ne("codeTaxon", null)
        )

        runMeasure(qFilter, qStr, nbIterations)
    }

    @Test def testQ2() {
        println("------------- testQ2 - Saint Pierre et Miquelon 4835 results -------------")
        val nbIterations = 10

        // Equivalent queries as string for Jongo and Java filter for Mongo
        val qStr = "{$where:'this.codeTaxon==this.codeReference','spm':{$ne:''},'spm':{$ne:null}, 'codeTaxon': {$exists: true, $ne: null}}"
        val qFilter = And(
            where("this.codeTaxon==this.codeReference"),
            exists("codeTaxon", true), Ne("codeTaxon", null),
            Ne("spm", ""), Ne("spm", null)
        )

        runMeasure(qFilter, qStr, nbIterations)
    }

    @Test def testQ3() {
        println("------------- testQ3 - Guadeloupe 17956 results -------------")
        val nbIterations = 10

        // Equivalent queries as string for Jongo and Java filter for Mongo
        val qStr = "{$where:'this.codeTaxon==this.codeReference','gua':{$ne:''},'gua':{$ne:null}, 'codeTaxon': {$exists: true, $ne: null}}"
        val qFilter = And(
            where("this.codeTaxon==this.codeReference"),
            exists("codeTaxon", true), Ne("codeTaxon", null),
            Ne("gua", ""), Ne("gua", null)
        )

        runMeasure(qFilter, qStr, nbIterations)
    }

    @Test def testQ4() {
        println("------------- testQ4 - Nouvelle-Calédonie 35703 results -------------")
        val nbIterations = 10

        // Equivalent queries as string for Jongo and Java filter for Mongo
        val qStr = "{$where:'this.codeTaxon==this.codeReference','nc':{$ne:''},'nc':{$ne:null}, 'codeTaxon': {$exists: true, $ne: null}}"
        val qFilter = And(
            where("this.codeTaxon==this.codeReference"),
            exists("codeTaxon", true), Ne("codeTaxon", null),
            Ne("nc", ""), Ne("nc", null)
        )

        runMeasure(qFilter, qStr, nbIterations)
    }
    
    @Test def testQ5() {
        println("------------- testQ5 - Metropole 128018 results -------------")
        val nbIterations = 10

        // Equivalent queries as string for Jongo and Java filter for Mongo
        val qStr = "{$where:'this.codeTaxon==this.codeReference','fr':{$ne:''},'fr':{$ne:null}, 'codeTaxon': {$exists: true, $ne: null}}"
        val qFilter = And(
            where("this.codeTaxon==this.codeReference"),
            exists("codeTaxon", true), Ne("codeTaxon", null),
            Ne("fr", ""), Ne("fr", null)
        )

        runMeasure(qFilter, qStr, nbIterations)
    }

    @Test def testQ6() {
        println("------------- testQ6 - 227224 results -------------")
        val nbIterations = 10

        // Equivalent queries as string for Jongo and Java filter for Mongo
        val qStr = "{$where:'this.codeTaxon==this.codeReference', 'codeTaxon': {$exists: true, $ne: null}}"
        val qFilter = And(
            where("this.codeTaxon==this.codeReference"),
            exists("codeTaxon", true), Ne("codeTaxon", null)
        )

        runMeasure(qFilter, qStr, nbIterations)
    }

    /**
     * Polynésie : <http://sws.geonames.org/4030656/> - 38773 results
     * Martinique : 3570311 - 24943 results
     * Guyane : 3381670 - 74114 results
     * 
     */
}
