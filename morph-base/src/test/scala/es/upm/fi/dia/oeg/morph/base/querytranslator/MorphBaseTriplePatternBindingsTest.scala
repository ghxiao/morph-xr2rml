package es.upm.fi.dia.oeg.morph.base.querytranslator

import org.junit.Assert
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import com.hp.hpl.jena.graph.NodeFactory
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLSubjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateMap
import com.hp.hpl.jena.datatypes.RDFDatatype
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap

class MorphBaseTriplePatternBindingsTest {

    @Test
    def test_compatible_VariableTpTerm {
        var variable = NodeFactory.createVariable("x")

        var termMap: R2RMLTermMap = new R2RMLObjectMap(
            Constants.MorphTermMapType.ConstantTermMap, // term map type
            Some(Constants.R2RML_LITERAL_URI), // term type
            None, // data type
            None, // language tag
            None, // xR2RMLNestedTermMap]
            "JSONPath") // Reference formulation
        termMap.setConstantValue("val")
        assertTrue(MorphBaseTriplePatternBindings.compatible(termMap, variable))

        termMap = new R2RMLSubjectMap(
            Constants.MorphTermMapType.ReferenceTermMap, // term map type
            Some(Constants.R2RML_IRI_URI), // term type
            Set.empty, // class URIs
            Set.empty, // graph URIs
            "JSONPath") // Reference formulation
        assertTrue(MorphBaseTriplePatternBindings.compatible(termMap, variable))

        termMap = new R2RMLPredicateMap(
            Constants.MorphTermMapType.ReferenceTermMap, // term map type
            Some(Constants.R2RML_BLANKNODE_URI), // term type
            "JSONPath") // Reference formulation
        assertTrue(MorphBaseTriplePatternBindings.compatible(termMap, variable))
    }

    @Test
    def test_compatible_ConstantTermMap_LiteralTpTerm {

        var literal = NodeFactory.createLiteral("3")
        var literal_lang = NodeFactory.createLiteral("3", "fr", false)
        var literal_langde = NodeFactory.createLiteral("3", "de", false)
        var literal_dt = NodeFactory.createLiteral("3", com.hp.hpl.jena.datatypes.xsd.XSDDatatype.XSDint)
        var literal_dtl = NodeFactory.createLiteral("3", com.hp.hpl.jena.datatypes.xsd.XSDDatatype.XSDlong)

        var termMap = new R2RMLObjectMap(
            Constants.MorphTermMapType.ConstantTermMap, // term map type
            Some(Constants.R2RML_LITERAL_URI), // term type
            None, // data type
            None, // language tag
            None, "JSONPath") // xR2RMLNestedTermMap, Reference formulation
        termMap.setConstantValue("3")
        assertTrue(MorphBaseTriplePatternBindings.compatible(termMap, literal))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal_lang))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal_dt))

        termMap = new R2RMLObjectMap(
            Constants.MorphTermMapType.ConstantTermMap, // term map type
            Some(Constants.R2RML_LITERAL_URI), // term type
            Some("http://www.w3.org/2001/XMLSchema#int"), // data type
            None, // language tag
            None, "JSONPath") // xR2RMLNestedTermMap, Reference formulation
        termMap.setConstantValue("3")
        assertTrue(MorphBaseTriplePatternBindings.compatible(termMap, literal_dt))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal_dtl))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal_lang))

        termMap = new R2RMLObjectMap(
            Constants.MorphTermMapType.ConstantTermMap, // term map type
            Some(Constants.R2RML_LITERAL_URI), // term type
            None, // data type
            Some("fr"), // language tag
            None, "JSONPath") // xR2RMLNestedTermMap, Reference formulation
        termMap.setConstantValue("3")
        assertTrue(MorphBaseTriplePatternBindings.compatible(termMap, literal_lang))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal_langde))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal_dt))
    }

    @Test
    def test_compatible_ConstantTermMap_IriTpTerm {
        var iri = NodeFactory.createURI("http://example.org/%20starring")
        var iri2 = NodeFactory.createURI("http://example.org/starring/star")
        var literal = NodeFactory.createLiteral("http://example.org/starring")
        var bn = NodeFactory.createAnon()

        var termMap = new R2RMLObjectMap(
            Constants.MorphTermMapType.ConstantTermMap, // term map type
            Some(Constants.R2RML_IRI_URI), // term type
            None, None, // data type, language tag
            None, "JSONPath") // xR2RMLNestedTermMap, Reference formulation
        termMap.setConstantValue("http://example.org/%20starring")
        assertTrue(MorphBaseTriplePatternBindings.compatible(termMap, iri))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, iri2))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, bn))
    }

    @Test
    def test_compatible_ReferenceTermMap_LiteralTpTerm {
        var literal = NodeFactory.createLiteral("3")
        var literal_lang = NodeFactory.createLiteral("3", "fr", false)
        var literal_langde = NodeFactory.createLiteral("3", "de", false)
        var literal_dt = NodeFactory.createLiteral("3", com.hp.hpl.jena.datatypes.xsd.XSDDatatype.XSDstring)
        var iri = NodeFactory.createURI("http://example.org/starring")
        var bn = NodeFactory.createAnon()

        var termMap = new R2RMLObjectMap(
            Constants.MorphTermMapType.ReferenceTermMap, // term map type
            Some(Constants.R2RML_LITERAL_URI), // term type
            None, None, // data type, language tag
            None, "JSONPath") // xR2RMLNestedTermMap, Reference formulation
        assertTrue(MorphBaseTriplePatternBindings.compatible(termMap, literal))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal_lang))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal_dt))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, iri))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, bn))

        termMap = new R2RMLObjectMap(
            Constants.MorphTermMapType.ReferenceTermMap, // term map type
            Some(Constants.R2RML_LITERAL_URI), // term type
            Some("http://www.w3.org/2001/XMLSchema#string"), // data type
            None, // language tag
            None, "JSONPath") // xR2RMLNestedTermMap, Reference formulation
        assertTrue(MorphBaseTriplePatternBindings.compatible(termMap, literal_dt))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal_lang))

        termMap = new R2RMLObjectMap(
            Constants.MorphTermMapType.ReferenceTermMap, // term map type
            Some(Constants.R2RML_LITERAL_URI), // term type
            None, // data type
            Some("fr"), // language tag
            None, "JSONPath") // xR2RMLNestedTermMap, Reference formulation
        assertTrue(MorphBaseTriplePatternBindings.compatible(termMap, literal_lang))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal_langde))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal_dt))
    }

    @Test
    def test_compatible_ReferenceTermMap_IriTpTerm {
        var iri = NodeFactory.createURI("http://example.org/%20starring")
        var literal = NodeFactory.createLiteral("val")
        var bn = NodeFactory.createAnon()

        var termMap = new R2RMLObjectMap(
            Constants.MorphTermMapType.ReferenceTermMap, // term map type
            Some(Constants.R2RML_IRI_URI), // term type
            None, None, // data type, language tag
            None, "JSONPath") // xR2RMLNestedTermMap, Reference formulation
        assertTrue(MorphBaseTriplePatternBindings.compatible(termMap, iri))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, bn))
    }

    @Test
    def test_compatible_TemplateTermMap_LiteralTpTerm {
        var literal = NodeFactory.createLiteral("http://example.org/starring/foo")
        var literal1 = NodeFactory.createLiteral("http://example.org/starring/foo/faa")
        var literal2 = NodeFactory.createLiteral("http://example.org/starring")
        var literal3 = NodeFactory.createLiteral("dav://example.org/starring/foo")
        var literal_lang = NodeFactory.createLiteral("http://example.org/starring/foo", "fr", false)
        var iri = NodeFactory.createURI("http://example.org/starring/foo")

        var termMap = new R2RMLObjectMap(
            Constants.MorphTermMapType.TemplateTermMap, // term map type
            Some(Constants.R2RML_LITERAL_URI), // term type
            None, None, // data type, language tag
            None, "JSONPath") // xR2RMLNestedTermMap, Reference formulation
        termMap.templateString = "http://example.org/{$.toto.*}/{$.x[5]}"
        assertTrue(MorphBaseTriplePatternBindings.compatible(termMap, literal))
        assertTrue(MorphBaseTriplePatternBindings.compatible(termMap, literal1))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal2))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal3))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal_lang))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, iri))
    }

    @Test
    def test_compatible_TemplateTermMap_IriTpTerm {
        var iri = NodeFactory.createURI("http://example.org/starring/foo")
        var iri1 = NodeFactory.createURI("http://example.org/starring/foo/faa")
        var iri2 = NodeFactory.createURI("http://example.org/starring")
        var iri3 = NodeFactory.createURI("dav://example.org/starring/foo")
        var literal = NodeFactory.createLiteral("http://example.org/starring/foo")

        var termMap = new R2RMLObjectMap(
            Constants.MorphTermMapType.TemplateTermMap, // term map type
            Some(Constants.R2RML_IRI_URI), // term type
            None, None, // data type, language tag
            None, "JSONPath") // xR2RMLNestedTermMap, Reference formulation
        termMap.templateString = "http://example.org/{$.toto.*}/{$.x[5]}"
        assertTrue(MorphBaseTriplePatternBindings.compatible(termMap, iri))
        assertTrue(MorphBaseTriplePatternBindings.compatible(termMap, iri1))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, iri2))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, iri3))
        assertFalse(MorphBaseTriplePatternBindings.compatible(termMap, literal))
    }

    @Test
    def test_compatible_TermMaps {
        // Compatible term maps with constant/reference/template

        var termMap1 = new R2RMLObjectMap(
            Constants.MorphTermMapType.ConstantTermMap, // term map type
            Some(Constants.R2RML_IRI_URI), // term type
            None, None, // data type, language tag
            None, "JSONPath") // xR2RMLNestedTermMap, Reference formulation
        assertTrue(MorphBaseTriplePatternBindings.compatibleTermMaps(termMap1, termMap1))

        var termMap2 = new R2RMLObjectMap(
            Constants.MorphTermMapType.ReferenceTermMap, // term map type
            Some(Constants.R2RML_IRI_URI), // term type
            None, None, // data type, language tag
            None, "JSONPath") // xR2RMLNestedTermMap, Reference formulation
        assertTrue(MorphBaseTriplePatternBindings.compatibleTermMaps(termMap1, termMap2))

        termMap2 = new R2RMLObjectMap(
            Constants.MorphTermMapType.TemplateTermMap, // term map type
            Some(Constants.R2RML_IRI_URI), // term type
            None, None, // data type, language tag
            None, "JSONPath") // xR2RMLNestedTermMap, Reference formulation
        assertTrue(MorphBaseTriplePatternBindings.compatibleTermMaps(termMap1, termMap2))
    }

    @Test
    def test_compatible_TermMaps2 {
        // Language tags, data types

        var termMap1 = new R2RMLObjectMap(
            Constants.MorphTermMapType.ReferenceTermMap, // term map type
            Some(Constants.R2RML_LITERAL_URI), // term type
            None, None, // data type, language tag
            None, "JSONPath") // xR2RMLNestedTermMap, Reference formulation
        assertTrue(MorphBaseTriplePatternBindings.compatibleTermMaps(termMap1, termMap1))

        var termMap2 = new R2RMLObjectMap(
            Constants.MorphTermMapType.ReferenceTermMap, // term map type
            Some(Constants.R2RML_LITERAL_URI), // term type
            None, Some("fr"), // data type, language tag
            None, "JSONPath") // xR2RMLNestedTermMap, Reference formulation
        assertTrue(MorphBaseTriplePatternBindings.compatibleTermMaps(termMap2, termMap2))
        assertFalse(MorphBaseTriplePatternBindings.compatibleTermMaps(termMap1, termMap2))

        termMap2 = new R2RMLObjectMap(
            Constants.MorphTermMapType.ReferenceTermMap, // term map type
            Some(Constants.R2RML_LITERAL_URI), // term type
            Some("http://www.w3.org/2001/XMLSchema#string"), None, // data type, language tag
            None, "JSONPath") // xR2RMLNestedTermMap, Reference formulation
        assertTrue(MorphBaseTriplePatternBindings.compatibleTermMaps(termMap2, termMap2))
        assertFalse(MorphBaseTriplePatternBindings.compatibleTermMaps(termMap1, termMap2))
    }

    @Test
    def test_compatible_TermMaps3 {
        // Literal, IRI, BlankNode 

        var termMap1 = new R2RMLObjectMap(
            Constants.MorphTermMapType.ReferenceTermMap, // term map type
            Some(Constants.R2RML_LITERAL_URI), // term type
            None, None, // data type, language tag
            None, "JSONPath") // xR2RMLNestedTermMap, Reference formulation

        // Language tags, data types
        var termMap2 = new R2RMLObjectMap(
            Constants.MorphTermMapType.ReferenceTermMap, // term map type
            Some(Constants.R2RML_IRI_URI), // term type
            None, None, // data type, language tag
            None, "JSONPath") // xR2RMLNestedTermMap, Reference formulation
        assertTrue(MorphBaseTriplePatternBindings.compatibleTermMaps(termMap2, termMap2))
        assertFalse(MorphBaseTriplePatternBindings.compatibleTermMaps(termMap1, termMap2))

        termMap2 = new R2RMLObjectMap(
            Constants.MorphTermMapType.ReferenceTermMap, // term map type
            Some(Constants.R2RML_BLANKNODE_URI), // term type
            None, None, // data type, language tag
            None, "JSONPath") // xR2RMLNestedTermMap, Reference formulation
        assertTrue(MorphBaseTriplePatternBindings.compatibleTermMaps(termMap2, termMap2))
        assertFalse(MorphBaseTriplePatternBindings.compatibleTermMaps(termMap1, termMap2))
    }
}