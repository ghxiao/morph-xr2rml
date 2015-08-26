package es.upm.fi.dia.oeg.morph.base

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.RDFNode

class GeneralUtilityTest {

    @Test def TestCompareRdfListOk() {
        println("------------------ TestCompareRdfListOk ------------------")
        val model = ModelFactory.createDefaultModel()

        val valuesAsRdfNodes = List("a", "b", "c").map(value => model.createLiteral(value))
        val lst1 = model.createList(valuesAsRdfNodes.toArray[RDFNode])

        val valuesAsRdfNodes2 = List("a", "b", "c").map(value => model.createLiteral(value))
        val lst2 = model.createList(valuesAsRdfNodes2.toArray[RDFNode])

        assertTrue(GeneralUtility.compareRdfList(lst1, lst2))
    }

    @Test def TestCompareRdfListNOk() {
        println("------------------ TestCompareRdfListNOk ------------------")
        val model = ModelFactory.createDefaultModel()

        val valuesAsRdfNodes = List("a", "b", "d").map(value => model.createLiteral(value))
        val lst1 = model.createList(valuesAsRdfNodes.toArray[RDFNode])

        val valuesAsRdfNodes2 = List("a", "b", "c").map(value => model.createLiteral(value))
        val lst2 = model.createList(valuesAsRdfNodes2.toArray[RDFNode])

        assertFalse(GeneralUtility.compareRdfList(lst1, lst2))
    }

    @Test def TestCompareRdfListDiffSize() {
        println("------------------ TestCompareRdfListDiffSize ------------------")
        val model = ModelFactory.createDefaultModel()

        val valuesAsRdfNodes = List("a", "b").map(value => model.createLiteral(value))
        val lst1 = model.createList(valuesAsRdfNodes.toArray[RDFNode])

        val valuesAsRdfNodes2 = List("a", "b", "c").map(value => model.createLiteral(value))
        val lst2 = model.createList(valuesAsRdfNodes2.toArray[RDFNode])

        assertFalse(GeneralUtility.compareRdfList(lst1, lst2))
        assertFalse(GeneralUtility.compareRdfList(lst2, lst1))
    }

    @Test def TestCompareRdfContainerOk() {
        println("------------------ TestCompareRdfContainerOk ------------------")
        val model = ModelFactory.createDefaultModel()

        val lst1 = model.createBag()
        for (value <- List("a", "b", "c")) lst1.add(value)
        
        val lst2 = model.createBag()
        for (value <- List("a", "b", "c")) lst2.add(value)
        
        assertTrue(GeneralUtility.compareRdfContainer(lst1, lst2))
    }

    @Test def TestCompareRdfContainerNOk() {
        println("------------------ TestCompareRdfContainerNOk ------------------")
        val model = ModelFactory.createDefaultModel()

        val lst1 = model.createBag()
        for (value <- List("a", "b", "d")) lst1.add(value)

        val lst2 = model.createBag()
        for (value <- List("a", "b", "c")) lst2.add(value)

        assertFalse(GeneralUtility.compareRdfContainer(lst1, lst2))
    }

    @Test def TestCompareRdfContainerDiffSize() {
        println("------------------ TestCompareRdfContainerDiffSize ------------------")
        val model = ModelFactory.createDefaultModel()

        val lst1 = model.createBag()
        for (value <- List("a", "b")) lst1.add(value)

        val lst2 = model.createBag()
        for (value <- List("a", "b", "c")) lst2.add(value)

        assertFalse(GeneralUtility.compareRdfContainer(lst1, lst2))
        assertFalse(GeneralUtility.compareRdfContainer(lst2, lst1))
    }

}