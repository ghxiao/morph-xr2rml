package fr.unice.i3s.morph.xr2rml.mongo.querytranslator

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test

import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.graph.Triple

import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunnerFactory
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryResultProcessor
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.base.querytranslator.TPBindings
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import fr.unice.i3s.morph.xr2rml.mongo.MongoDBQuery
import fr.unice.i3s.morph.xr2rml.mongo.abstractquery.AbstractAtomicQuery
import fr.unice.i3s.morph.xr2rml.mongo.abstractquery.AbstractQueryInnerJoinRef
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeAnd
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCompare
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCondEquals
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCondExists
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeElemMatch
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeField
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeOr
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeUnion
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryProjectionArraySlice

class MorphFactoryConcret2 extends MorphBaseRunnerFactory {

    override def createConnection: GenericConnection = null
    override def createUnfolder: MorphBaseUnfolder = null
    override def createDataSourceReader: MorphBaseDataSourceReader = null
    override def createDataTranslator: MorphBaseDataTranslator = null
    override def createQueryTranslator: MorphBaseQueryTranslator = null
    override def createQueryResultProcessor: MorphBaseQueryResultProcessor = null
}

class MorphMongoQueryTranslatorTest {

    var props = MorphProperties.apply("src/test/resources/query_translator", "morph.properties")
    var mappingDocument = R2RMLMappingDocument(props, null)

    val factory = new MorphFactoryConcret2
    factory.mappingDocument = mappingDocument
    factory.properties = props
    var queryTranslator = new MorphMongoQueryTranslator(factory)

    val tmMovies = mappingDocument.getClassMappingsByName("Movies")
    val tmDirectors = mappingDocument.getClassMappingsByName("Directors")
    val tmOther = mappingDocument.getClassMappingsByName("Other")

    private def cleanString(str: String) = str.trim.replaceAll("\\s", "")

    @Test def test_mongoAbstractQuerytoConcrete_Field() {
        println("------ test_mongoAbstractQuerytoConcrete_Field")

        val field = new MongoQueryNodeField("b", List(new MongoQueryNodeCondEquals("bbb")))
        val field2 = new MongoQueryNodeField("b.0.b", List(new MongoQueryNodeCondEquals("b0b")))

        println("---------------------------------")
        var fromPart = new MongoDBQuery("collection", "tititutu")
        var result = queryTranslator.mongoAbstractQuerytoConcrete(fromPart, Set.empty, field)
        println(result)
        assertTrue(result.size == 1)
        assertTrue(cleanString(result(0).query).contains("tititutu"))
        assertTrue(cleanString(result(0).query).contains("'b':{$eq:'bbb'}"))
    }

    @Test def test_mongoAbstractQuerytoConcrete() {
        println("------ test_mongoAbstractQuerytoConcrete")
        var fromPart = new MongoDBQuery("collection", "tititutu")

        val compare = new MongoQueryNodeField("a", new MongoQueryNodeCompare(MongoQueryNodeCompare.Operator.GT, "10"))
        val field2 = new MongoQueryNodeField("b.0.b", List(new MongoQueryNodeCondEquals("b0b")))
        val exists1 = new MongoQueryNodeField("c", new MongoQueryNodeCondExists)
        val exists2 = new MongoQueryNodeField("d", new MongoQueryNodeCondExists)
        val or = new MongoQueryNodeOr(List(exists1, exists2))
        val and2 = new MongoQueryNodeAnd(List(compare, field2))
        val union = new MongoQueryNodeUnion(List(compare, or))

        println("---------------------------------")
        // Remove top-level AND
        var result = queryTranslator.mongoAbstractQuerytoConcrete(fromPart, Set.empty, and2)
        println(result)
        assertTrue(result.size == 1)
        assertTrue(cleanString(result(0).query).contains("tititutu"))
        assertTrue(cleanString(result(0).query).contains("'a':{$gt:10}"))
        assertTrue(cleanString(result(0).query).contains("'b.0.b':{$eq:'b0b'}"))

        println("---------------------------------")
        result = queryTranslator.mongoAbstractQuerytoConcrete(fromPart, Set.empty, new MongoQueryNodeAnd(List(compare, field2)))
        println(result)
        assertTrue(result.size == 1)
        assertTrue(cleanString(result(0).query).contains("tititutu"))
        assertTrue(cleanString(result(0).query).contains("'a':{$gt:10}"))
        assertTrue(cleanString(result(0).query).contains("'b.0.b':{$eq:'b0b'}"))

        println("---------------------------------")
        // Remove top-level AND from AND(OR, AND)
        result = queryTranslator.mongoAbstractQuerytoConcrete(fromPart, Set.empty, new MongoQueryNodeAnd(List(or, and2)))
        println(result)
        assertTrue(result.size == 1)
        assertTrue(cleanString(result(0).query).contains("tititutu"))
        assertTrue(cleanString(result(0).query).contains("'a':{$gt:10}"))
        assertTrue(cleanString(result(0).query).contains("'b.0.b':{$eq:'b0b'}"))
        assertTrue(cleanString(result(0).query).contains("$or:[{'c':{$exists:true}},{'d':{$exists:true}}]"))

        println("---------------------------------")
        // Top-level UNION
        result = queryTranslator.mongoAbstractQuerytoConcrete(fromPart, Set.empty, union)
        println(result)
        assertTrue(result.size == 2)
        assertTrue(cleanString(result(0).query).contains("tititutu"))
        assertTrue(cleanString(result(0).query).contains("'a':{$gt:10}"))
        assertTrue(cleanString(result(1).query).contains("tititutu"))
        assertTrue(cleanString(result(1).query).contains("$or:[{'c':{$exists:true}},{'d':{$exists:true}}]"))
    }

    @Test def test_mongoAbstractQuerytoConcrete_FusionQueries() {
        println("------ test_mongoAbstractQuerytoConcrete_FusionQueries")
        var fromPart = new MongoDBQuery("collection", "")

        val compare = new MongoQueryNodeField("a.b", new MongoQueryNodeCompare(MongoQueryNodeCompare.Operator.GT, "10"))
        val exists = new MongoQueryNodeField("a.b", new MongoQueryNodeCondExists)
        val field = new MongoQueryNodeField("c", List(new MongoQueryNodeCondEquals("ccc")))

        println("---------------------------------")
        var result = queryTranslator.mongoAbstractQuerytoConcrete(fromPart, Set.empty, new MongoQueryNodeAnd(List(compare, field, exists)))
        println(result)
        assertTrue(result.size == 1)
        assertTrue(cleanString(result(0).query).contains("'a.b':{$gt:10,$exists:true}"))
        assertTrue(cleanString(result(0).query).contains("'c':{$eq:'ccc'}"))

        println("---------------------------------")
        result = queryTranslator.mongoAbstractQuerytoConcrete(fromPart, Set.empty, new MongoQueryNodeAnd(List(compare, field, exists)))
        println(result)
        assertTrue(result.size == 1)
        assertTrue(cleanString(result(0).query).contains("'a.b':{$gt:10,$exists:true}"))
        assertTrue(cleanString(result(0).query).contains("'c':{$eq:'ccc'}"))

    }

    @Test def test_mongoAbstractQuerytoConcrete_FusionQueries_ElemMatch() {
        println("------ test_mongoAbstractQuerytoConcrete_FusionQueries_ElemMatch")
        var fromPart = new MongoDBQuery("collection", "")

        val elemmatch = new MongoQueryNodeField(
            "a.b",
            List(new MongoQueryNodeElemMatch(new MongoQueryNodeCompare(MongoQueryNodeCompare.Operator.GT, "10"))),
            List(new MongoQueryProjectionArraySlice("a.b", "10")))
        val elemmatch2 = new MongoQueryNodeField(
            "a.b", new MongoQueryNodeElemMatch(new MongoQueryNodeCompare(MongoQueryNodeCompare.Operator.LT, "20")))
        val arraysize = new MongoQueryNodeField(
            "a.b", new MongoQueryNodeCompare(MongoQueryNodeCompare.Operator.SIZE, "99"))

        // 2 arrays with one slice
        var result = queryTranslator.mongoAbstractQuerytoConcrete(fromPart, Set.empty, new MongoQueryNodeAnd(List(elemmatch, elemmatch2)))
        println(result(0).toString)
        assertTrue(result.size == 1)
        assertTrue(cleanString(result(0).query).contains("{'a.b':{$elemMatch:{$gt:10,$lt:20}}}"))
        assertTrue(cleanString(result(0).projection).contains("{'a.b':{$slice:10}}"))

        println("---------------------------------")
        // 2 arrays with one slice and one size
        result = queryTranslator.mongoAbstractQuerytoConcrete(fromPart, Set.empty, new MongoQueryNodeAnd(List(elemmatch, elemmatch2, arraysize)))
        println(result(0).toString)
        assertTrue(result.size == 1)
        assertTrue(cleanString(result(0).query).contains("{'a.b':{$size:99,$elemMatch:{$gt:10,$lt:20}}}"))
        assertTrue(cleanString(result(0).projection).contains("{'a.b':{$slice:10}}"))
    }

    @Test def test_transTPm_NonNormalizedTM() {
        println("------ test_transTPm_NonNormalizedTM")

        // Triple pattern: ?x ex:starring "T. Leung"
        val s = NodeFactory.createVariable("x")
        val p = NodeFactory.createURI("http://example.org/starring")
        val o = NodeFactory.createLiteral("T. Leung")
        val tp = Triple.create(s, p, o)

        try {
            var tm = mappingDocument.getClassMappingsByName("TM_NoPOM")
            var res = queryTranslator.transTPm(new TPBindings(tp, List(tm)))
            fail()
        } catch { case e: Exception => {} }

        try {
            var tm = mappingDocument.getClassMappingsByName("TM_MultiplePOM")
            var res = queryTranslator.transTPm(new TPBindings(tp, List(tm)))
            fail()
        } catch { case e: Exception => {} }
    }

    @Test def test_transTPm_noparentTM() {
        println("------ test_transTPm_noparentTM")

        // Triple pattern: ?x ex:starring "T. Leung"
        val s = NodeFactory.createVariable("x")
        val p = NodeFactory.createURI("http://example.org/starring")
        val o = NodeFactory.createLiteral("T. Leung")
        val tp = Triple.create(s, p, o)

        val Q = queryTranslator.transTPm(new TPBindings(tp, List(tmMovies)))
        assertTrue(Q.isInstanceOf[AbstractAtomicQuery])
        val q = Q.asInstanceOf[AbstractAtomicQuery]
        assertEquals(q.from.getValue, "db.movies.find({decade:{$exists:true}})")
        assertTrue(q.project.head.references.contains("$.code"))
        assertEquals("?x", q.project.head.as.get)
    }

    @Test def test_transTPm_parentTM() {
        println("------ test_transTPm_parentTM")

        // Triple pattern: ?x ex:directed <http://example.org/movie/Manh>
        val s = NodeFactory.createVariable("x")
        val p = NodeFactory.createURI("http://example.org/directed")
        val o = NodeFactory.createURI("http://example.org/movie/Manh")
        val tp = Triple.create(s, p, o)

        val Q = queryTranslator.transTPm(new TPBindings(tp, List(tmDirectors)))
        assertTrue(Q.isInstanceOf[AbstractQueryInnerJoinRef])
        val q = Q.asInstanceOf[AbstractQueryInnerJoinRef]
        assertEquals("$.directed.*", q.childRef)
        assertEquals("$.dirname", q.parentRef)
    }
}