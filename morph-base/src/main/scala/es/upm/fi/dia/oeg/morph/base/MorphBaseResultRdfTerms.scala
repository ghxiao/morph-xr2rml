package es.upm.fi.dia.oeg.morph.base

import com.hp.hpl.jena.rdf.model.RDFNode

/**
 * Representation of a triple as a set of RDF terms translated from database query results.
 *
 * Attributes subjectAsVariable, predicateAsVariable and objectAsVariable
 * keep track of the SPARQL variable to which each term is bound, if any.
 *
 * @author Franck Michel, I3S laboratory
 */
class MorphBaseResultRdfTerms(
        val subject: RDFNode, val subjectAsVariable: Option[String],
        val predicate: RDFNode, val predicateAsVariable: Option[String],
        val objct: RDFNode, val objectAsVariable: Option[String]) {

    /**
     * Simple triple id that helps keep track of which triples have already been materialized, to avoid duplicates
     */
    val id = (subject.toString + predicate.toString + objct.toString).hashCode()

    def hasVariable(variable: String): Boolean = {
        (subjectAsVariable.isDefined && subjectAsVariable.get == variable) ||
            (predicateAsVariable.isDefined && predicateAsVariable.get == variable) ||
            (objectAsVariable.isDefined && objectAsVariable.get == variable)
    }

    /**
     * Retrieve an RDF term in the triple that is bound to a given variable.
     * If several terms are bound to the same variable, like in triples matching pattern like 
     * <code>?x :pred ?x</code>,
     * both the subject and object will have the same value. So there is no need to return them all.
     */
    def getTermForVariable(variable: String): Option[RDFNode] = {
        if (subjectAsVariable.isDefined && subjectAsVariable.get == variable)
            Some(subject)
        else if (predicateAsVariable.isDefined && predicateAsVariable.get == variable)
            Some(predicate)
        else if (objectAsVariable.isDefined && objectAsVariable.get == variable)
            Some(objct)
        else None
    }

    override def toString = {
        "[" + subject.toString +
            { if (subjectAsVariable.isDefined) " AS " + subjectAsVariable.get else "" } + ",\n " +
            predicate.toString +
            { if (predicateAsVariable.isDefined) " AS " + predicateAsVariable.get else "" } + ",\n" +
            objct.toString +
            { if (objectAsVariable.isDefined) " AS " + objectAsVariable.get else "" } + "]"
    }
}