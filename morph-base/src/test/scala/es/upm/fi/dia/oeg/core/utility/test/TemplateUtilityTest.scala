package es.upm.fi.dia.oeg.core.utility.test

import scala.util.matching.Regex
import scala.collection.mutable.HashMap
import es.upm.fi.dia.oeg.morph.base.TemplateUtility

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class TemplateUtilityTest {

    @Test def TestGetTemplateColumns1() {
        val tpl = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/{ar}/{nr}";
        val colRefs = TemplateUtility.getTemplateColumns(tpl);
        println("Found columns " + colRefs + " in template " + tpl)
        assertEquals("ar", colRefs(0))
        assertEquals("nr", colRefs(1))
    }

    @Test def TestGetTemplateColumns() {
        val xPath = """XPath(\/\/root\/node[1]\(\)\/@id)"""
        val jsonPath = """JSONPath($['store'].book[\(@.length-1\)].title)"""
        val mixedPath = "Column(NAME)/CSV(3)/" + xPath + "/" + jsonPath + "/TSV(name)"
        var tpl = "http://example.org/student/{ID}/{" + mixedPath + "}/{ID2}/{" + mixedPath + "}"

        val colRefs = TemplateUtility.getTemplateColumns(tpl)
        println("colRefs: " + colRefs)

        assertEquals("ID", colRefs(0))
        assertEquals("NAME", colRefs(1))
        assertEquals("ID2", colRefs(2))
        assertEquals("NAME", colRefs(3))
    }
}