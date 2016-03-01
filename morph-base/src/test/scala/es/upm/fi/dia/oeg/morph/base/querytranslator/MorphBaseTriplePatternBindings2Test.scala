package es.upm.fi.dia.oeg.morph.base.querytranslator

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.sparql.algebra.Op
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.query.MorphAbstractQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import com.hp.hpl.jena.sparql.algebra.Algebra

class MorphBaseTriplePatternBindings2Concret extends MorphBaseQueryTranslator {

    this.properties = MorphProperties.apply("src/test/resources/query_translator", "morph_test_bindings.properties")
    this.mappingDocument = R2RMLMappingDocument("query_translator/mapping_test_bindings.ttl", properties, null)

    override def translate(op: Op): MorphAbstractQuery = { null }

    override def transTPm(tp: Triple, tmSet: List[R2RMLTriplesMap]): MorphAbstractQuery = {
        throw new MorphException("Not supported")
    }
}

class MorphBaseTriplePatternBindings2Test {

    var queryTranslator = new MorphBaseTriplePatternBindings2Concret()

    MorphBaseTriplePatternBindings.mappingDocument = queryTranslator.mappingDocument

    val TM1 = queryTranslator.mappingDocument.getClassMappingsByName("TM1_plop")
    val TM2 = queryTranslator.mappingDocument.getClassMappingsByName("TM2_plop")
    val TM3 = queryTranslator.mappingDocument.getClassMappingsByName("TM3_plop")
    val TM4 = queryTranslator.mappingDocument.getClassMappingsByName("TM4_plip")
    val TM5 = queryTranslator.mappingDocument.getClassMappingsByName("TM4_plup")
    val TM6 = queryTranslator.mappingDocument.getClassMappingsByName("TM4_plup")

    @Test def test_join() {
        println("------ test_join")

        var s1 = NodeFactory.createVariable("x")
        var p1 = NodeFactory.createURI("http://example.org/starring")
        var o1 = NodeFactory.createVariable("y")
        var tp1 = Triple.create(s1, p1, o1)

        var s2 = NodeFactory.createVariable("x")
        var p2 = NodeFactory.createURI("http://example.org/starring")
        var o2 = NodeFactory.createVariable("z")
        var tp2 = Triple.create(s2, p2, o2)

        val compat = MorphBaseTriplePatternBindings.join(tp1, List(TM1), tp2, List(TM2, TM3))
        assertTrue(compat.contains((TM1, TM2)))
        assertFalse(compat.contains((TM1, TM3))) // subject template strings are incompatible
    }

    @Test def test_join2() {
        println("------ test_join2")

        var s1 = NodeFactory.createVariable("x")
        var p1 = NodeFactory.createURI("http://example.org/starring")
        var o1 = NodeFactory.createVariable("y")
        var tp1 = Triple.create(s1, p1, o1)

        var s2 = NodeFactory.createURI("http://example.org/turlu")
        var p2 = NodeFactory.createURI("http://example.org/starring")
        var o2 = NodeFactory.createVariable("y")
        var tp2 = Triple.create(s2, p2, o2)

        val compat = MorphBaseTriplePatternBindings.join(tp1, List(TM1, TM2), tp2, List(TM2, TM3))
        assertTrue(compat.contains((TM1, TM3)))
        assertTrue(compat.contains((TM2, TM2)))
        assertFalse(compat.contains((TM1, TM2))) // datatype in object map of TM2 and not in TM1 
    }

    @Test def test_join3() {
        println("------ test_join3")

        var s1 = NodeFactory.createVariable("x")
        var p1 = NodeFactory.createURI("http://example.org/starring")
        var o1 = NodeFactory.createVariable("x")
        var tp1 = Triple.create(s1, p1, o1)

        var s2 = NodeFactory.createVariable("x")
        var p2 = NodeFactory.createURI("http://example.org/starring")
        var o2 = NodeFactory.createVariable("y")
        var tp2 = Triple.create(s2, p2, o2)

        val compat = MorphBaseTriplePatternBindings.join(tp1, List(TM1, TM2), tp2, List(TM2, TM3))
        assertTrue(compat.isEmpty)
    }

    @Test def test_join4() {
        // With a RefObjectMap
        println("------ test_join4")

        var s1 = NodeFactory.createVariable("x")
        var p1 = NodeFactory.createURI("http://example.org/directed")
        var o1 = NodeFactory.createVariable("y")
        var tp1 = Triple.create(s1, p1, o1)

        var s2 = NodeFactory.createVariable("y")
        var p2 = NodeFactory.createURI("http://example.org/starring")
        var o2 = NodeFactory.createVariable("z")
        var tp2 = Triple.create(s2, p2, o2)

        val compat = MorphBaseTriplePatternBindings.join(tp1, List(TM4), tp2, List(TM2, TM3))
        assertTrue(compat.contains((TM4, TM2)))
        assertFalse(compat.contains((TM4, TM3)))
    }

    @Test def test_bindm() {
        println("------ test_bindm")
        var query = """
			PREFIX ex: <http://example.org/>			
			SELECT ?x ?y WHERE {
            	?x ex:plop ?y .
			}"""

        var sparqlQuery = QueryFactory.create(query, null, null)
        var bindings = MorphBaseTriplePatternBindings.bindm(Algebra.compile(sparqlQuery))
        var res = bindings.values.toList.map(_.bound).flatten
        assertTrue(res.contains(TM1))
        assertTrue(res.contains(TM2))
        assertTrue(res.contains(TM3))
    }

    @Test def test_bindm2() {
        println("------ test_bindm2")
        var query = """
			PREFIX ex: <http://example.org/>			
			SELECT ?y WHERE {
            	<http://example.org/plop/1> ex:plop ?y .
			}"""

        var sparqlQuery = QueryFactory.create(query, null, null)
        var bindings = MorphBaseTriplePatternBindings.bindm(Algebra.compile(sparqlQuery))
        var res = bindings.values.toList.map(_.bound).flatten
        assertTrue(res.contains(TM1))
        assertTrue(res.contains(TM2))
        assertFalse(res.contains(TM3))
    }

    @Test def test_bindm3() {
        println("------ test_bindm3")
        var query = """
			PREFIX ex: <http://example.org/>			
			SELECT ?y WHERE {
            	?x ex:plip ?y .
            	?y ex:plop ?z .
			}"""

        var sparqlQuery = QueryFactory.create(query, null, null)
        var bindings = MorphBaseTriplePatternBindings.bindm(Algebra.compile(sparqlQuery))

        var s1 = NodeFactory.createVariable("x")
        var p1 = NodeFactory.createURI("http://example.org/plip")
        var o1 = NodeFactory.createVariable("y")
        var tp1 = Triple.create(s1, p1, o1)

        var s2 = NodeFactory.createVariable("y")
        var p2 = NodeFactory.createURI("http://example.org/plop")
        var o2 = NodeFactory.createVariable("z")
        var tp2 = Triple.create(s2, p2, o2)

        var res = bindings(tp2.toString).bound
        assertTrue(res.size == 2)
        assertTrue(res.contains(TM1))
        assertTrue(res.contains(TM2))
        assertFalse(res.contains(TM3))

        res = bindings(tp1.toString).bound
        assertTrue(res.contains(TM4))
        assertTrue(res.size == 1)
    }

    @Test def test_bindm10() {
        println("------ test_bindm")
        val query = """
			PREFIX ex: <http://example.org/>
			
			SELECT ?x ?y WHERE {
            	#?x ex:plip <http://example.org/plop/1> .
            	?x ex:plip ?y .
            	?y ex:plop ?z .
                OPTIONAL { ?y ex:plup "plup" . }
			}"""

        val sparqlQuery = QueryFactory.create(query, null, null)
        val bindings = MorphBaseTriplePatternBindings.bindm(Algebra.compile(sparqlQuery))
        println(bindings)
    }
}