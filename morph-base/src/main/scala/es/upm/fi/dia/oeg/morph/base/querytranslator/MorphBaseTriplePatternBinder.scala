package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.seqAsJavaList
import org.apache.log4j.Logger
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.sparql.algebra.Op
import com.hp.hpl.jena.sparql.algebra.op.OpBGP
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct
import com.hp.hpl.jena.sparql.algebra.op.OpFilter
import com.hp.hpl.jena.sparql.algebra.op.OpJoin
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin
import com.hp.hpl.jena.sparql.algebra.op.OpOrder
import com.hp.hpl.jena.sparql.algebra.op.OpProject
import com.hp.hpl.jena.sparql.algebra.op.OpSlice
import com.hp.hpl.jena.sparql.algebra.op.OpUnion
import com.hp.hpl.jena.sparql.core.BasicPattern
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.TemplateUtility
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.base.engine.IMorphFactory

/**
 * Representation of the bindings of several triples map to a triple pattern
 */
class TPBindings(val tp: Triple, val bound: List[R2RMLTriplesMap]) {
    override def toString = { bound.toString }
}

object TPBindings {
    def apply(tp: Triple, bound: List[R2RMLTriplesMap]) = { (tp.toString -> new TPBindings(tp, bound)) }
}

/**
 * Simple representation of a binding of one triples map to a triple pattern
 */
class TPBinding(val tp: Triple, val bound: R2RMLTriplesMap) {
    override def toString = { "Binding(" + tp + " -> " + bound + ")" }

    override def equals(q: Any): Boolean = {
        q.isInstanceOf[TPBinding] && {
            val tpb = q.asInstanceOf[TPBinding]
            this.tp == tpb.tp && this.bound != null && tpb.bound != null && this.bound.toString == tpb.bound.toString
        }
    }

}

/**
 * Implementation of the different functions to figure out which triples maps are bound to a triple pattern,
 * i.e. those triples maps that are possible candidates to generate RDF triples matching the triple pattern.
 *
 * See full definitions in https://hal.archives-ouvertes.fr/hal-01245883.
 */
class MorphBaseTriplePatternBinder(factory: IMorphFactory) {

    val logger = Logger.getLogger(this.getClass());

    /**
     * Compute the triple pattern bindings for all triple patterns in a graph pattern
     *
     * @param op SPARQL query or SPARQL query element
     * @return bindings as a map of triples and associated triples maps
     */
    def bindm(op: Op): Map[String, TPBindings] = {

        op match {

            case opProject: OpProject => { // SELECT clause
                bindm(opProject.getSubOp())
            }

            case bgp: OpBGP => { // Basic Graph Pattern
                val triples: List[Triple] = bgp.getPattern.getList.toList
                if (triples.size == 0)
                    Map.empty
                else {
                    // Compute the bindings of the first triple pattern
                    val tp1 = triples.head
                    var bindingsTp1 = bindmTP(tp1)
                    val result = Map(TPBindings(tp1, bindingsTp1))
                    if (logger.isTraceEnabled()) logger.trace("Binding of single triple pattern: " + result)

                    if (triples.size == 1) {
                        // Only one triple pattern in the BGP, return its bindings straight away
                        result
                    } else {
                        // Several triple patterns in the BGP 
                        var results: Map[String, TPBindings] = Map.empty

                        // Compute the bindings of the remaining triples
                        val rightTps = new OpBGP(BasicPattern.wrap(triples.tail))
                        val rightBindings = bindm(rightTps)

                        // For each of the remaining triples, join its bindings with those of tp1
                        for (rightBinding <- rightBindings) {
                            val tp2 = rightBinding._2.tp
                            val bindingsTp2 = rightBinding._2.bound

                            if (haveSharedVariables(tp1, tp2)) {
                                // There are shared variables between tp1 and tp2
                                val joined = join(tp1, bindingsTp1, tp2, bindingsTp2)
                                results = results + TPBindings(tp2, reduceRight(joined)) // reduce the bindings of tp2
                                bindingsTp1 = reduceLeft(joined) // reduce the bindings of tp1
                            } else { // There is no shared variable between tp1 and tp2
                                if (results.contains(tp2.toString))
                                    results = results - tp2.toString + TPBindings(tp2, bindingsTp2 intersect results(tp2.toString).bound) // keep all the bindings of tp2
                                else
                                    results = results + TPBindings(tp2, bindingsTp2) // keep all the bindings of tp2
                            }
                        }
                        // Finally add the bindings for tp1
                        if (results.contains(tp1.toString))
                            results = results - tp1.toString + TPBindings(tp1, bindingsTp1 intersect results(tp1.toString).bound)
                        else
                            results = results + TPBindings(tp1, bindingsTp1)
                        if (logger.isDebugEnabled()) logger.debug("Binding of BGP: " + results)
                        results
                    }
                }
            }

            case opJoin: OpJoin => { // AND pattern
                var results: Map[String, TPBindings] = Map.empty

                // Compute the bindings of the two graph patterns
                val leftBindings = this.bindm(opJoin.getLeft)
                val rightBindings = this.bindm(opJoin.getRight)

                for (leftBinding <- leftBindings) {
                    val tp1 = leftBinding._2.tp
                    var bindingsTp1 = leftBinding._2.bound

                    for (rightBinding <- rightBindings) {
                        val tp2 = rightBinding._2.tp
                        val bindingsTp2 = rightBinding._2.bound

                        if (haveSharedVariables(tp1, tp2)) {
                            // There are shared variables between tp1 and tp2
                            val joined =
                                if (results.contains(tp2.toString)) {
                                    val restp2 = results(tp2.toString)
                                    results = results - tp2.toString // Remove the current bindings of tp2 
                                    // Intersect the current bindings of tp2 and the right bindings of tp2
                                    join(tp1, bindingsTp1, tp2, bindingsTp2 intersect restp2.bound)
                                } else
                                    join(tp1, bindingsTp1, tp2, bindingsTp2)

                            bindingsTp1 = reduceLeft(joined) // reduce the bindings of tp1

                            if (results.contains(tp2.toString))
                                results = results - tp2.toString + TPBindings(tp2, reduceRight(joined) intersect results(tp2.toString).bound) // reduce the bindings of tp2
                            else
                                results = results + TPBindings(tp2, reduceRight(joined)) // reduce the bindings of tp2
                        } else {
                            // There is no shared variable between tp1 and tp2: keep all the bindings of tp2
                            if (results.contains(tp2.toString))
                                results = results - tp2.toString + TPBindings(tp2, bindingsTp2 intersect results(tp2.toString).bound)
                            else
                                results = results + TPBindings(tp2, bindingsTp2)
                        }
                    }

                    // Add the bindings for tp1
                    if (results.contains(tp1.toString))
                        results = results - tp1.toString + TPBindings(tp1, bindingsTp1 intersect results(tp1.toString).bound)
                    else
                        results = results + TPBindings(tp1, bindingsTp1)
                }
                if (logger.isDebugEnabled()) logger.debug("Binding of AND graph pattern: " + results)
                results
            }

            case opLeftJoin: OpLeftJoin => { //OPT pattern
                var results: Map[String, TPBindings] = Map.empty

                // Compute the bindings of the two graph patterns
                val leftBindings = this.bindm(opLeftJoin.getLeft)
                val rightBindings = this.bindm(opLeftJoin.getRight)

                for (leftBinding <- leftBindings) {
                    val tp1 = leftBinding._2.tp
                    val bindingsTp1 = leftBinding._2.bound

                    for (rightBinding <- rightBindings) {
                        val tp2 = rightBinding._2.tp
                        val bindingsTp2 = rightBinding._2.bound

                        if (haveSharedVariables(tp1, tp2)) {
                            // There are shared variables between tp1 and tp2
                            val joined =
                                if (results.contains(tp2.toString)) {
                                    val restp2 = results(tp2.toString).bound
                                    results = results - tp2.toString // Remove the current bindings of tp2 
                                    // Intersect the current bindings of tp2 and the right bindings of tp2
                                    join(tp1, bindingsTp1, tp2, bindingsTp2 intersect restp2)
                                } else
                                    join(tp1, bindingsTp1, tp2, bindingsTp2)

                            results = results + TPBindings(tp2, reduceRight(joined)) // reduce the bindings of tp2
                        } else {
                            // There is no shared variable between tp1 and tp2
                            if (results.contains(tp2.toString))
                                results = results - tp2.toString + TPBindings(tp2, bindingsTp2 intersect results(tp2.toString).bound) // keep all the bindings of tp2
                            else
                                results = results + TPBindings(tp2, bindingsTp2) // keep all the bindings of tp2
                        }

                        // Add the bindings for tp1: case where tp1 would show in left and right graph patterns
                        if (results.contains(tp1.toString))
                            results = results - tp1.toString + TPBindings(tp1, bindingsTp1 intersect results(tp1.toString).bound)
                        else
                            results = results + TPBindings(tp1, bindingsTp1)
                    }
                }
                if (logger.isDebugEnabled()) logger.debug("Binding of OPTIONAL graph pattern: " + results)
                results
            }

            case opUnion: OpUnion => { //UNION pattern
                var results: Map[String, TPBindings] = Map.empty
                val leftBindings = this.bindm(opUnion.getLeft)
                val rightBindings = this.bindm(opUnion.getRight)

                for (leftBinding <- leftBindings) {
                    val tp1 = leftBinding._2.tp
                    val bindingsTp1 = leftBinding._2.bound
                    if (results.contains(tp1.toString))
                        results = results - tp1.toString + TPBindings(tp1, bindingsTp1 union results(tp1.toString).bound)
                    else
                        results = results + TPBindings(tp1, bindingsTp1)

                    for (rightBinding <- rightBindings) {
                        val tp2 = rightBinding._2.tp
                        val bindingsTp2 = rightBinding._2.bound
                        if (results.contains(tp2.toString))
                            results = results - tp2.toString + TPBindings(tp2, bindingsTp2 union results(tp2.toString).bound)
                        else
                            results = results + TPBindings(tp2, bindingsTp2)
                    }
                }
                if (logger.isDebugEnabled()) logger.debug("Binding of UNION graph pattern: " + results)
                results
            }

            case opFilter: OpFilter => { //FILTER pattern
                Map.empty
            }
            case opSlice: OpSlice => {
                Map.empty
            }
            case opDistinct: OpDistinct => {
                Map.empty
            }
            case opOrder: OpOrder => {
                Map.empty
            }
            case _ => {
                Map.empty
            }
        }
    }

    /**
     * Compute the list of triples maps TM that are bound to triple pattern tp.
     * TM is bound to tp if and only if:
     * 		compatible(TM.sub, tp.sub) ^ compatible(TM.pred, tp.pred) ^ compatible(TM.obj, tp.obj)}
     */
    private def bindmTP(tp: Triple): List[R2RMLTriplesMap] = {

        val boundTMs = factory.getMappingDocument.triplesMaps.toList.map(tm => {
            val pom = tm.predicateObjectMaps.head

            if (tm.predicateObjectMaps.size > 1 || pom.predicateMaps.size > 1 ||
                pom.objectMaps.size > 1 || pom.refObjectMaps.size > 1)
                throw new MorphException("Error, non-normalized triples map: " + tm)

            var bound: Boolean = compatible(tm.subjectMap, tp.getSubject) &&
                compatible(pom.predicateMaps.head, tp.getPredicate) &&
                (if (pom.hasObjectMap)
                    compatible(pom.objectMaps.head, tp.getObject)
                else if (pom.hasRefObjectMap)
                    compatible(pom.refObjectMaps.head, tp.getObject)
                else {
                    logger.warn("Unormalized triples map " + tm.toString + ": no object maps nor referencing object maps")
                    false
                })

            if (bound) Some(tm) else None

        }).flatten // flatten removes None, keeping possibly an empty list of bound triples maps

        boundTMs
    }

    /**
     * Join the bindings of two triple patterns on the set of variables that they share.
     *
     * join(tp1, boundTp1, tp2, boundTp2) is the set of pairs (TM1, TM2) of boundTp1 x boundTp2, such that
     * for each shared variable v, it holds that compatibleTermMaps(TM1.postp1(v), TM2.postp2(v)),
     * in other words the term map of TM1 at the position of v in tp1 is compatible with the term map of TM2
     * at the position of v in tp2.
     *
     * If there is no shared variable, return an empty result.
     *
     * See full definitions in https://hal.archives-ouvertes.fr/hal-01245883.
     *
     * @param tp1 triple pattern 1
     * @param boundTp1 set of triples maps that are bound to tp1
     * @param tp2 triple pattern 2
     * @param boundTp2 set of triples maps that are bound to tp2
     * @return list of pairs of triples maps from tp1 and tp2 that are compatible with each other
     */
    def join(tp1: Triple, boundTp1: List[R2RMLTriplesMap], tp2: Triple, boundTp2: List[R2RMLTriplesMap]): List[(R2RMLTriplesMap, R2RMLTriplesMap)] = {

        // Compute the set of variables in tp1 (varsTp1) and keep track of their position in (varPosTp1)
        var varPosTp1 = getTpVarsPos(tp1)
        var varsTp1 = getTpVars(tp1)

        // Compute the set of variables in tp2 (varsTp2) and keep track of their position in (varPosTp2)
        var varPosTp2 = getTpVarsPos(tp2)
        var varsTp2 = getTpVars(tp2)

        // Compute the set of variables shared by tp1 and tp2
        var result: List[(R2RMLTriplesMap, R2RMLTriplesMap)] = List.empty
        val sharedVars = varsTp1.intersect(varsTp2)
        for (x <- sharedVars) {
            if (logger.isTraceEnabled())
                logger.trace("Checking compatibility of term maps for variable " + x)

            // Search the positions of variable x in each triple pattern
            // e.g.: ?x :prop ?x => positions sub and obj
            val xInTp1 = varPosTp1.filter(q => q._2 == x).map(_._1) // positions of variable x in tp1
            val xInTp2 = varPosTp2.filter(q => q._2 == x).map(_._1) // positions of variable x in tp2

            for (tm1 <- boundTp1)
                for (tm2 <- boundTp2) {
                    // For (tm1, tm2) to be an acceptable result their term maps must be compatible 
                    // at all positions of variable x in tp1 and tp2
                    var compat = true
                    for (postp1 <- xInTp1)
                        for (postp2 <- xInTp2) {
                            val c = compatibleTermMaps(getTermMapAtPos(tm1, postp1), getTermMapAtPos(tm2, postp2))
                            if (logger.isTraceEnabled())
                                if (c)
                                    logger.trace(tm1.toString + "." + postp1 + " compatible with " + tm2 + "." + postp2)
                                else
                                    logger.trace(tm1.toString + "." + postp1 + " incompatible with " + tm2 + "." + postp2)

                            compat = compat && compatibleTermMaps(getTermMapAtPos(tm1, postp1), getTermMapAtPos(tm2, postp2))
                        }
                    if (compat)
                        result = result :+ (tm1, tm2)
                }
        }
        if (logger.isDebugEnabled())
            logger.debug("Joined bindings of [" + tp1 + "] and [" + tp2 + "]:\n" + result)
        result
    }

    private def reduceLeft(joinedTM: List[(R2RMLTriplesMap, R2RMLTriplesMap)]): List[R2RMLTriplesMap] = {
        joinedTM.map(_._1).distinct
    }

    private def reduceRight(joinedTM: List[(R2RMLTriplesMap, R2RMLTriplesMap)]): List[R2RMLTriplesMap] = {
        joinedTM.map(_._2).distinct
    }

    /**
     * Check the compatibility between a term map and a triple pattern term.
     *
     * Note in case of a RefObjectMap: if the RefObjectMap has a termType (xrr:RdfList, xrr:RdfBag etc.)
     * we could also verify that the members of the tpTerm (list/bag etc.) have a termType,
     * language tag or datatype compatible with the definition of the xxr:nestedTermType of the RefObjectMam.
     * We choose not to go there as we assume this shall be rare.
     *
     * @param tm an instance of R2RMLTermMap or R2RMLRefObjectMap
     * @return true if compatible
     * @throws MorphException if tm is neither an R2RMLTermMap or R2RMLRefObjectMap
     */
    def compatible(tm: Object, tpTerm: Node): Boolean = {

        if (tm.isInstanceOf[R2RMLTermMap]) {

            // ---- Case of an xR2RML TermMap
            val termMap = tm.asInstanceOf[R2RMLTermMap]
            if (tpTerm.isVariable())
                // A variable is always compatible with a term map
                true
            else {
                // Check type of tpTerm vs. termType of term map
                val termType = termMap.inferTermType
                var incompatible: Boolean =
                    (tpTerm.isLiteral && termType != Constants.R2RML_LITERAL_URI) ||
                        (tpTerm.isURI && termType != Constants.R2RML_IRI_URI) ||
                        (tpTerm.isBlank && (termType != Constants.R2RML_BLANKNODE_URI && !termMap.isRdfCollectionTermType))

                if (tpTerm.isLiteral) {
                    // Check language tag
                    val L = tpTerm.getLiteralLanguage
                    if (L == null || L.isEmpty())
                        incompatible = incompatible || (termMap.languageTag.isDefined)
                    else
                        incompatible = incompatible ||
                            (!termMap.languageTag.isDefined || (termMap.languageTag.isDefined && termMap.languageTag.get != L))

                    // Check data type
                    val T = tpTerm.getLiteralDatatypeURI
                    if (T == null || T.isEmpty())
                        incompatible = incompatible || (termMap.datatype.isDefined)
                    else
                        incompatible = incompatible ||
                            (!termMap.datatype.isDefined || (termMap.datatype.isDefined && termMap.datatype.get != T))
                }

                // Constant term map and the triple pattern term does not have the same value
                incompatible = incompatible ||
                    (termMap.isConstantValued && tpTerm.isLiteral && termMap.getConstantValue != tpTerm.getLiteralValue.toString) ||
                    (termMap.isConstantValued && tpTerm.isURI && termMap.getConstantValue != tpTerm.getURI)

                // Template term map and the triple pattern does not match the template string
                incompatible = incompatible ||
                    termMap.isTemplateValued && tpTerm.isLiteral && termMap.getTemplateValues(tpTerm.getLiteralValue.toString).isEmpty ||
                    termMap.isTemplateValued && tpTerm.isURI && termMap.getTemplateValues(tpTerm.getURI).isEmpty

                !incompatible
            }
        } else if (tm.isInstanceOf[R2RMLRefObjectMap]) {

            // ---- Case of an xR2RML RefObjectMap
            val termMap = tm.asInstanceOf[R2RMLRefObjectMap]
            if (tpTerm.isVariable())
                // A variable is always compatible with a term map
                true
            else if (termMap.termType.isDefined && !tpTerm.isBlank)
                // An xR2RML RefObjectMap may have a termType, but only a RDF collection or list term type. 
                // In that case, it necessarily produces blank nodes.
                false
            else {
                val parentTM = factory.getMappingDocument.getParentTriplesMap(termMap)
                compatible(parentTM.subjectMap, tpTerm)
            }
        } else
            throw new MorphException("Method compatible: object passed is neither an R2RMLTermMap or R2RMLRefObjectMap: " + tm)
    }

    /**
     * Check the compatibility between two term maps
     *
     * @param tm1 an instance of R2RMLTermMap or R2RMLRefObjectMap
     * @param tm2 an instance of R2RMLTermMap or R2RMLRefObjectMap
     * @return true if compatible
     * @throws MorphException if tm1 or tm2 is neither an R2RMLTermMap or R2RMLRefObjectMap
     */
    def compatibleTermMaps(tm1: Object, tm2: Object): Boolean = {

        if (tm1.isInstanceOf[R2RMLTermMap]) {

            val termMap1 = tm1.asInstanceOf[R2RMLTermMap]
            if (tm2.isInstanceOf[R2RMLTermMap]) {
                // ---- tm1 and tm2 are TermMaps
                val termMap2 = tm2.asInstanceOf[R2RMLTermMap]
                var incompatible: Boolean =
                    (termMap1.inferTermType != termMap2.inferTermType) ||
                        (termMap1.languageTag != termMap2.languageTag) ||
                        (termMap1.datatype != termMap2.datatype) ||
                        (termMap1.isTemplateValued && termMap2.isTemplateValued &&
                            !TemplateUtility.compatibleTemplateStrings(termMap1.templateString, termMap2.templateString))
                !incompatible

            } else if (tm2.isInstanceOf[R2RMLRefObjectMap]) {
                // ---- tm1 is a TermMap and tm2 is a RefObjectMap
                val termMap2 = tm2.asInstanceOf[R2RMLRefObjectMap]
                compatibleTermMaps(termMap1, factory.getMappingDocument.getParentTriplesMap(termMap2).subjectMap)
            } else
                throw new MorphException("tm2 is neither an R2RMLTermMap or R2RMLRefObjectMap: " + tm2)

        } else if (tm1.isInstanceOf[R2RMLRefObjectMap]) {

            val termMap1 = tm1.asInstanceOf[R2RMLRefObjectMap]
            if (tm2.isInstanceOf[R2RMLTermMap]) {
                // ---- tm1 is a RefObjectMMap and tm2 is a TermMap
                val termMap2 = tm2.asInstanceOf[R2RMLTermMap]
                compatibleTermMaps(factory.getMappingDocument.getParentTriplesMap(termMap1).subjectMap, termMap2)

            } else if (tm2.isInstanceOf[R2RMLRefObjectMap]) {
                // ---- tm1 and tm2 are RefObjectMMaps
                val termMap2 = tm2.asInstanceOf[R2RMLRefObjectMap]
                compatibleTermMaps(factory.getMappingDocument.getParentTriplesMap(termMap1).subjectMap, factory.getMappingDocument.getParentTriplesMap(termMap2).subjectMap)
            } else
                throw new MorphException("tm2 is neither an R2RMLTermMap or R2RMLRefObjectMap: " + tm2)
        } else
            throw new MorphException("tm1 is neither an R2RMLTermMap or R2RMLRefObjectMap: " + tm1)
    }

    /**
     * Return the term map at position pos (subject, predicate or object) or a triples map.
     * The triples map must be normalized.
     *
     * @param tm a triple map
     * @param pos sub, pre or obj
     * @return an instance of R2RMLSubjectMap, R2RMLPredicatetMap, R2RMLObjectMap or R2RMLRefObjectMap
     */
    private def getTermMapAtPos(tm: R2RMLTriplesMap, pos: Constants.MorphPOS.Value): Object = {
        pos match {
            case Constants.MorphPOS.sub => tm.subjectMap

            case Constants.MorphPOS.pre => {
                val poms = tm.predicateObjectMaps
                if (poms.isEmpty || poms.size > 1)
                    throw new MorphException("Unormalized triples map " + tm.toString + " must have exactly one predicate-object map.")
                val pom = poms.head
                if (pom.predicateMaps.isEmpty)
                    throw new MorphException("Unormalized triples map " + tm.toString + " must have exactly one PredicateMap.")
                pom.predicateMaps.head
            }

            case Constants.MorphPOS.obj => {
                val poms = tm.predicateObjectMaps
                if (poms.isEmpty || poms.size > 1)
                    throw new MorphException("Unormalized triples map " + tm.toString + " must have exactly one predicate-object map.")
                val pom = poms.head
                if (!pom.objectMaps.isEmpty)
                    pom.objectMaps.head
                else if (!pom.refObjectMaps.isEmpty)
                    pom.refObjectMaps.head
                else
                    throw new MorphException("Unormalized triples map " + tm.toString + " must have exactly one ObjectMap or one RefObjectMap .")
            }

            case Constants.MorphPOS.graph =>
                throw new MorphException("Unexpected retrieval of graph map for triples map " + tm.toString)
        }
    }

    /**
     * Get the list of variables in a triple pattern
     */
    private def getTpVars(tp: Triple): List[String] = {
        var varsTp: Set[String] = Set.empty

        if (tp.getSubject.isVariable)
            varsTp = varsTp + tp.getSubject.toString
        if (tp.getPredicate.isVariable)
            varsTp = varsTp + tp.getPredicate.toString
        if (tp.getObject.isVariable)
            varsTp = varsTp + tp.getObject.toString

        varsTp.toList
    }

    /**
     * Get the list of variables in a triple pattern along with their position
     */
    private def getTpVarsPos(tp: Triple): Map[Constants.MorphPOS.Value, String] = {
        var varPosTp: Map[Constants.MorphPOS.Value, String] = Map.empty

        if (tp.getSubject.isVariable)
            varPosTp = varPosTp + (Constants.MorphPOS.sub -> tp.getSubject.toString)
        if (tp.getPredicate.isVariable)
            varPosTp = varPosTp + (Constants.MorphPOS.pre -> tp.getPredicate.toString)
        if (tp.getObject.isVariable)
            varPosTp = varPosTp + (Constants.MorphPOS.obj -> tp.getObject.toString)

        varPosTp
    }

    /**
     * Check whether two triple patterns have at least one shared variable
     */
    private def haveSharedVariables(tp1: Triple, tp2: Triple): Boolean = {
        !(getTpVars(tp1).intersect(getTpVars(tp2)).isEmpty)
    }
}
