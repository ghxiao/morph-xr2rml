package fr.unice.i3s.morph.xr2rml.mongo

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeExists
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeAnd
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeField
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNode
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLQuery

class MongoDBQueryTest {

    @Test def testMostSpecificQuery_empty() {
        println("------------------------------------------------- testMostSpecificQuery_empty")

        var ls1 = new xR2RMLQuery("db.collection.find()", "JSONPath", None)
        var ls2 = new xR2RMLQuery("db.collection.find()", "JSONPath", None)

        var mostSpec = MongoDBQuery.mostSpecificQuery(ls2, ls1)
        assertTrue(mostSpec.isDefined)
        println(mostSpec.get)
        assertEquals("db.collection.find({})", mostSpec.get.query)

        ls2 = new xR2RMLQuery("db.collection.find({})", "JSONPath", None)

        mostSpec = MongoDBQuery.mostSpecificQuery(ls1, ls2)
        assertTrue(mostSpec.isDefined)
        println(mostSpec.get)
        assertEquals("db.collection.find({})", mostSpec.get.query)
    }

    @Test def testMostSpecificQuery() {
        println("------------------------------------------------- testMostSpecificQuery")

        var ls1 = new xR2RMLQuery("db.collection.find({query1, query2})", "JSONPath", None)
        var ls2 = new xR2RMLQuery("db.collection.find({query1, query2})", "JSONPath", None)

        var mostSpec = MongoDBQuery.mostSpecificQuery(ls2, ls1)
        assertTrue(mostSpec.isDefined)
        println(mostSpec.get)
        assertEquals("db.collection.find({query1,query2})", mostSpec.get.query)

        ls1 = new xR2RMLQuery("db.collection.find({query1, query2})", "JSONPath", None)
        ls2 = new xR2RMLQuery("db.collection.find({query1, query2, query3})", "JSONPath", None)

        mostSpec = MongoDBQuery.mostSpecificQuery(ls1, ls2)
        assertTrue(mostSpec.isDefined)
        println(mostSpec.get)
        assertEquals("db.collection.find({query1,query2,query3})", mostSpec.get.query)

        mostSpec = MongoDBQuery.mostSpecificQuery(ls2, ls1)
        assertTrue(mostSpec.isDefined)
        println(mostSpec.get)
        assertEquals("db.collection.find({query1,query2,query3})", mostSpec.get.query)
    }

    @Test def testMostSpecificQuery2() {
        println("------------------------------------------------- testMostSpecificQuery2")

        var ls1 = new xR2RMLQuery("db.collection.find({query1, query2})", "JSONPath", None)
        var ls2 = new xR2RMLQuery("db.collection.find({query1, query2})", "XPath", None)

        var mostSpec = MongoDBQuery.mostSpecificQuery(ls1, ls2)
        assertFalse(mostSpec.isDefined)

        mostSpec = MongoDBQuery.mostSpecificQuery(ls2, ls1)
        assertFalse(mostSpec.isDefined)

        ls2 = new xR2RMLQuery("db.collection.find({query1, query2})", "XPath", Some("iter"))
        mostSpec = MongoDBQuery.mostSpecificQuery(ls2, ls1)
        assertFalse(mostSpec.isDefined)

    }

}