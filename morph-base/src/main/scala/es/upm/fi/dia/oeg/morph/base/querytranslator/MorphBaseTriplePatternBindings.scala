package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions.asScalaBuffer

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

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.TemplateUtility
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap

/**
 * Implementation of the different functions to figure out which triples maps are bound to a triple pattern,
 * i.e. those triples maps that are possible candidates to generate RDF triples matching the triple pattern.
 *
 * See full definitions in https://hal.archives-ouvertes.fr/hal-01245883.
 */
object MorphBaseTriplePatternBindings {

    var mappingDocument: R2RMLMappingDocument = null // set in MorphBaseQueryTranslator constructor

    val logger = Logger.getLogger(this.getClass());

    /**
     * Compute the triple pattern bindings for all triple patterns in a graph pattern
     *
     * @param op SPARQL query or SPARQL query element
     * @return bindings as a map of triples and associated triples maps
     */
    def bindm(op: Op): Map[Triple, List[R2RMLTriplesMap]] = {

        op match {
            case opProject: OpProject => { // SELECT clause
                val subOp = opProject.getSubOp();
                this.bindm(subOp)
            }
            case bgp: OpBGP => { // Basic Graph Pattern
                val triples: List[Triple] = bgp.getPattern.getList.toList
                if (triples.size == 0)
                    Map.empty
                else if (triples.size == 1) {
                    // Single triple pattern
                    val tp = triples.head
                    val boundTMs = this.mappingDocument.triplesMaps.toList.map(tm => {

                        // triples map TM is bound to tp iff
                        //    compatible(TM.sub, tp.sub) ^ compatible(TM.pred, tp.pred) ^ compatible(TM.obj, tp.obj)}
                        val pom = tm.predicateObjectMaps.head
                        var bound = MorphBaseTriplePatternBindings.compatible(tm.subjectMap, tp.getSubject) &&
                            MorphBaseTriplePatternBindings.compatible(pom.predicateMaps.head, tp.getPredicate) &&
                            (if (pom.hasObjectMap)
                                MorphBaseTriplePatternBindings.compatible(pom.objectMaps.head, tp.getObject)
                            else if (pom.hasRefObjectMap)
                                MorphBaseTriplePatternBindings.compatible(pom.refObjectMaps.head, tp.getObject)
                            else {
                                logger.warn("Unormalized triples map " + tm.toString + ": no object maps nor referencing object maps")
                                false
                            })

                        if (bound) Some(tm) else None
                    }).flatten // flatten removes None, keeping possibly an empty list of bound triples maps
                    Map(tp -> boundTMs)
                } else {
                    // @TODO
                    // Multiple triple patterns in a BGP
                    Map.empty
                }
            }
            case opJoin: OpJoin => { // AND pattern
                Map.empty
            }
            case opLeftJoin: OpLeftJoin => { //OPT pattern
                Map.empty
            }
            case opUnion: OpUnion => { //UNION pattern
                val left = bindm(opUnion.getLeft)
                val right = bindm(opUnion.getRight)
                left ++ right
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
     * Compute the pairs of triples maps bound to tp1 and tp2 that are compatible for the shared variables.
     * See full definitions in https://hal.archives-ouvertes.fr/hal-01245883.
     *
     * @param tp1 triple pattern 1
     * @param boundTp1 set of triples maps that are bound to tp1
     * @param tp2 triple pattern 2
     * @param boundTp2 set of triples maps that are bound to tp2
     */
    def join(tp1: Triple, boundTp1: List[R2RMLTriplesMap], tp2: Triple, boundTp2: List[R2RMLTriplesMap]): List[(R2RMLTriplesMap, R2RMLTriplesMap)] = {

        val sub = Constants.MorphPOS.sub
        val pre = Constants.MorphPOS.pre
        val obj = Constants.MorphPOS.obj

        // Compute the set of variables in tp1 (varsTp1) and keep track of their position in (varPosTp1)
        var varPosTp1: Map[Constants.MorphPOS.Value, String] = Map.empty
        var varsTp1: Set[String] = Set.empty
        if (tp1.getSubject.isVariable) {
            varsTp1 = varsTp1 + tp1.getSubject.toString
            varPosTp1 = varPosTp1 + (sub -> tp1.getSubject.toString)
        }
        if (tp1.getPredicate.isVariable) {
            varsTp1 = varsTp1 + tp1.getPredicate.toString
            varPosTp1 = varPosTp1 + (pre -> tp1.getPredicate.toString)
        }
        if (tp1.getObject.isVariable) {
            varsTp1 = varsTp1 + tp1.getObject.toString
            varPosTp1 = varPosTp1 + (obj -> tp1.getObject.toString)
        }

        // Compute the set of variables in tp2 (varsTp2) and keep track of their position in (varPosTp2)
        var varPosTp2: Map[Constants.MorphPOS.Value, String] = Map.empty
        var varsTp2: Set[String] = Set.empty
        if (tp2.getSubject.isVariable) {
            varsTp2 = varsTp2 + tp2.getSubject.toString
            varPosTp2 = varPosTp2 + (sub -> tp2.getSubject.toString)
        }
        if (tp2.getPredicate.isVariable) {
            varsTp2 = varsTp2 + tp2.getPredicate.toString
            varPosTp2 = varPosTp2 + (pre -> tp2.getPredicate.toString)
        }
        if (tp2.getObject.isVariable) {
            varsTp2 = varsTp2 + tp2.getObject.toString
            varPosTp2 = varPosTp2 + (obj -> tp2.getObject.toString)
        }

        // Compute the set of variables shared by tp1 and tp2
        val sharedVars = varsTp1.intersect(varsTp2)

        var result: List[(R2RMLTriplesMap, R2RMLTriplesMap)] = List.empty

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
                val parentTM = mappingDocument.getParentTriplesMap(termMap)
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
                compatibleTermMaps(termMap1, mappingDocument.getParentTriplesMap(termMap2).subjectMap)
            } else
                throw new MorphException("tm2 is neither an R2RMLTermMap or R2RMLRefObjectMap: " + tm2)

        } else if (tm1.isInstanceOf[R2RMLRefObjectMap]) {

            val termMap1 = tm1.asInstanceOf[R2RMLRefObjectMap]
            if (tm2.isInstanceOf[R2RMLTermMap]) {
                // ---- tm1 is a RefObjectMMap and tm2 is a TermMap
                val termMap2 = tm2.asInstanceOf[R2RMLTermMap]
                compatibleTermMaps(mappingDocument.getParentTriplesMap(termMap1).subjectMap, termMap2)

            } else if (tm2.isInstanceOf[R2RMLRefObjectMap]) {
                // ---- tm1 and tm2 are RefObjectMMaps
                val termMap2 = tm2.asInstanceOf[R2RMLRefObjectMap]
                compatibleTermMaps(mappingDocument.getParentTriplesMap(termMap1).subjectMap, mappingDocument.getParentTriplesMap(termMap2).subjectMap)
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
}
