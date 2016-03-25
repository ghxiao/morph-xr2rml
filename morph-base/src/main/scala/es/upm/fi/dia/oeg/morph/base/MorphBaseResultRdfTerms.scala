package es.upm.fi.dia.oeg.morph.base

import com.hp.hpl.jena.rdf.model.RDFNode

/**
 * Set of RDF terms translated from database query results.
 *
 * Attributes subjectAsVariable, predicateAsVariable and objectAsVariable
 * keep track of the SPARQL variable to which each term is attached, if any.
 */
class MorphBaseResultRdfTerms(
        val subjects: List[RDFNode], val subjectAsVariable: Option[String],
        val predicates: List[RDFNode], val predicateAsVariable: Option[String],
        val objects: List[RDFNode], val objectAsVariable: Option[String],
        val graphs: List[RDFNode]) {

    val id = subjects.toString + predicates.toString + objects.toString + graphs.toString
        
    def hasVariable(variable: String): Boolean = {
        (subjectAsVariable.isDefined && subjectAsVariable.get == variable) ||
            (predicateAsVariable.isDefined && predicateAsVariable.get == variable) ||
            (objectAsVariable.isDefined && objectAsVariable.get == variable)
    }

    def getTermsForVariable(variable: String): List[RDFNode] = {
        if (subjectAsVariable.isDefined && subjectAsVariable.get == variable)
            subjects
        else if (predicateAsVariable.isDefined && predicateAsVariable.get == variable)
            predicates
        else if (objectAsVariable.isDefined && objectAsVariable.get == variable)
            objects
        else List.empty
    }

    override def toString = {
        "[" + subjects.toString +
            { if (subjectAsVariable.isDefined) " AS " + subjectAsVariable.get else "" } + ",\n " +
            predicates.toString +
            { if (predicateAsVariable.isDefined) " AS " + predicateAsVariable.get else "" } + ",\n" +
            objects.toString +
            { if (objectAsVariable.isDefined) " AS " + objectAsVariable.get else "" } + ",\n" +
            graphs.toString + "]"
    }
}