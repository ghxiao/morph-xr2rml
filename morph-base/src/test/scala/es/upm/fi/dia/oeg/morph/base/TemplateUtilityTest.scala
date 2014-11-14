package es.upm.fi.dia.oeg.morph.base

import scala.util.matching.Regex
import scala.collection.mutable.HashMap
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class TemplateUtilityTest {

    @Test def TestGetTemplateGroups() {
        println("------------------ TestGetTemplateGroups ------------------")
        val xPath = """XPath(\/\/root\/node[1]\(\)\/@id)"""
        val jsonPath = """JSONPath($['store'].book[\(@.length-1\)].title)"""
        val mixedPath = "Column(NAME)/CSV(3)/" + xPath + "/" + jsonPath + "/TSV(name)"
        var tpl = "http://example.org/student/{ID}/{" + mixedPath + "}/{ID2}/{" + mixedPath + "}"

        val groups = TemplateUtility.getTemplateGroups(tpl)
        println("groups: " + groups)

        assertEquals("ID", groups(0))
        assertEquals(mixedPath, groups(1))
        assertEquals("ID2", groups(2))
        assertEquals(mixedPath, groups(3))
    }

    @Test def TestGetTemplateColumns1() {
        println("------------------ TestGetTemplateColumns1 ------------------")
        val tpl = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/{ar}/{nr}";
        val colRefs = TemplateUtility.getTemplateColumns(tpl);
        println("Found columns " + colRefs + " in template " + tpl)
        assertEquals("ar", colRefs(0))
        assertEquals("nr", colRefs(1))
    }

    @Test def TestGetTemplateColumns2() {
        println("------------------ TestGetTemplateColumns2 ------------------")
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