package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.log4j.Logger

import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.algebra.Algebra
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
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.TemplateUtility
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.query.MorphAbstractQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap

/**
 * Abstract class for the engine that shall translate a SPARQL query into a concrete database query
 *
 * @author Franck Michel (franck.michel@cnrs.fr)
 */
abstract class MorphBaseQueryTranslator {

    var genCnx: GenericConnection = null;

    var optimizer: MorphBaseQueryOptimizer = null;

    var properties: MorphProperties = null;

    var databaseType: String = null;

    var mappingDocument: R2RMLMappingDocument = null;

    val logger = Logger.getLogger(this.getClass());

    /** Map of nodes of a SPARQL query and the candidate triples maps for each node **/
    var mapInferredTMs: Map[Node, Set[R2RMLTriplesMap]] = Map.empty;

    MorphBaseTriplePatternBindings.mappingDocument = this.mappingDocument

    //Optimize.setFactory(new MorphQueryRewritterFactory());

    /**
     * High level entry point to the query translation process.
     *
     * @param sparqlQuery the SPARQL query to translate
     * @return set of concrete database queries.
     * @return a MorphAbstractQuery instance in which the targetQuery parameter has been set with
     * a list containing a set of concrete queries. In the RDB case there is only one query.
     * In the MongoDB case there may be several queries, which means a union of the results of all queries.
     * The result may be empty but not null.
     */
    def translate(sparqlQuery: Query): MorphAbstractQuery = {
        val start = System.currentTimeMillis()
        val result = this.translate(Algebra.compile(sparqlQuery));
        logger.info("Query translation time = " + (System.currentTimeMillis() - start) + "ms.");
        result
    }

    protected def translate(op: Op): MorphAbstractQuery

    /**
     * Translation of a triple pattern into an abstract query under a set of xR2RML triples maps
     *
     * @param tp a SPARQL triple pattern
     * @param tmSet a set of triples map that are bound to tp, i.e. they are candidates
     * that may potentially generate triples matching tp
     * @return abstract query. This may be an UNION if there are multiple triples maps,
     * and this may contain INNER JOINs for triples maps that have a referencing object map (parent triples map).
     * If there is only one triples map and no parent triples map, the result is an atomic abstract query.
     * @throws MorphException
     */
    def transTPm(tp: Triple, tmSet: List[R2RMLTriplesMap]): MorphAbstractQuery

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
                                logger.warn("Unormalized triples map " + tm.toString + ": cannot have both object maps and referencing object maps")
                                false
                            })

                        if (bound) Some(tm) else None
                    }).flatten // flatten removes None, keeping possibly an empty list of triples maps
                    Map(tp -> boundTMs)
                } else {
                    // @TODO
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
     * Generate the list of xR2RML references that are evaluated when generating the triples that match tp.
     * Those references are used to select the data elements to mention in the projection part of the
     * target database query.
     *
     * @param tp a SPARQL triple pattern
     * @param tm a triples map that has been assessed as a candidate triples map for the translation of tp into a query
     * @return list of projections
     * @throws MorphException if the triples map has no object map
     */
    def genProjection(tp: Triple, tm: R2RMLTriplesMap): List[MorphBaseQueryProjection] = {

        var listRefs: List[MorphBaseQueryProjection] = List.empty

        if (tp.getSubject().isVariable())
            listRefs = listRefs :+ new MorphBaseQueryProjection(tm.subjectMap.getReferences, Some(tp.getSubject.toString))

        val pom = tm.getPropertyMappings.head
        if (tp.getPredicate().isVariable())
            listRefs = listRefs :+ new MorphBaseQueryProjection(pom.predicateMaps.head.getReferences, Some(tp.getPredicate.toString))

        if (pom.hasRefObjectMap) {
            // The joined fields must always be projected, whether tp.obj is an IRI or a variable.
            // Useless for an RDB, but necessary for MongoDB since it cannot compute joins itself.
            val rom = pom.getRefObjectMap(0)
            for (jc <- rom.joinConditions)
                listRefs = listRefs :+ new MorphBaseQueryProjection(jc.childRef)
        } else if (tp.getObject().isVariable()) {
            listRefs = listRefs :+ new MorphBaseQueryProjection(pom.objectMaps.head.getReferences, Some(tp.getObject.toString))
        }
        listRefs
    }

    /**
     * Generate the list of xR2RML references that are evaluated when generating the triples that match tp.
     * Those references are used to select the data elements to mention in the projection part of the
     * target database query.
     * This version applies to the parent triples map of a relation child/parent triples map.
     *
     * @param tp a SPARQL triple pattern
     * @param tm a triples map that has been assessed as a candidate triples map for the translation of tp into a query
     * @return list of projections
     * @thrown MorphException if the triples map has no referencing object map
     */
    def genProjectionParent(tp: Triple, tm: R2RMLTriplesMap): List[MorphBaseQueryProjection] = {

        var listRefs: List[MorphBaseQueryProjection] = List.empty

        val pom = tm.getPropertyMappings.head
        if (!pom.hasRefObjectMap)
            throw new MorphException("Triples map " + tm + " has no parent triples map")

        // The joined fields must always be projected, whether tp.obj is an IRI or a variable: since MongoDB
        // cannot compute joins, the xR2RML processor has to do it, thus joined fields must be returned by both queries.
        val rom = pom.getRefObjectMap(0)
        for (jc <- rom.joinConditions) {
            listRefs = listRefs :+ new MorphBaseQueryProjection(jc.parentRef)
        }

        // In addition, if tp.obj is a variable, the subject of the parent TM must be projected too.
        if (tp.getObject().isVariable()) {
            val parentTM = mappingDocument.getParentTriplesMap(rom)
            listRefs = listRefs :+ new MorphBaseQueryProjection(parentTM.subjectMap.getReferences, Some(tp.getObject.toString))
        }

        listRefs
    }

    /**
     * Generate the set of conditions to apply to the database query (the from part), so that triples map TM
     * generates triples that match tp.
     * If a triple pattern term is constant (IRI or literal), genCond generates an equality condition,
     * only if the term map is reference-valued or template-valued. Indeed, if the term map is constant-valued and
     * the tp term is constant too, we should already have checked that they were compatible in the bind_m function.
     *
     * If a triple pattern term is variable, genCond generates a not-null condition, only
     * if the term map is reference-valued or template-valued. Indeed, if the term map is constant-valued, then the variable
     * will simply be bound to the constant value of the term map but that does not entail any condition in the query.
     *
     * A condition is either isNotNull(JSONPath expression) or equals(JSONPath expression, value)
     *
     * @param tp a SPARQL triple pattern
     * @param tm a triples map that has been assessed as a candidate triples map for the translation of tp into a query
     * @return set of conditions, either non-null or equality
     * @throws MorphException if the triples map has no object map
     */
    def genCond(tp: Triple, tm: R2RMLTriplesMap): List[MorphBaseQueryCondition] = {

        var conditions: List[MorphBaseQueryCondition] = List.empty

        { // === Subject map ===
            val tpTerm = tp.getSubject
            val termMap = tm.subjectMap

            if (termMap.isReferenceOrTemplateValued)
                if (tpTerm.isVariable)
                    // Add a not-null condition for each reference in the term map
                    conditions = conditions ++ termMap.getReferences.map(ref => MorphBaseQueryCondition.notNull(ref))
                else if (tpTerm.isURI) {
                    // Add an equality condition for each reference in the term map
                    conditions = conditions ++ genEqualityConditions(termMap, tpTerm)
                }
        }
        { // === Predicate map ===
            val tpTerm = tp.getPredicate
            val termMap = tm.predicateObjectMaps.head.predicateMaps.head

            if (termMap.isReferenceOrTemplateValued)
                if (tpTerm.isVariable)
                    // Add a not-null condition for each reference in the term map
                    conditions = conditions ++ termMap.getReferences.map(ref => MorphBaseQueryCondition.notNull(ref))
                else if (tpTerm.isURI)
                    // Add an equality condition for each reference in the term map
                    conditions = conditions ++ genEqualityConditions(termMap, tpTerm)
        }
        { // === Object map ===
            val tpTerm = tp.getObject
            val pom = tm.predicateObjectMaps.head

            if (tpTerm.isLiteral) {
                // Add an equality condition for each reference in the term map
                if (! pom.hasObjectMap)
                    throw new MorphException("Triples map " + tm + " has no object map. Presumably an inccorect triple pattern binding.")
                val termMap = pom.objectMaps.head
                if (termMap.isReferenceOrTemplateValued)
                    conditions = conditions ++ genEqualityConditions(termMap, tpTerm)

            } else if (tpTerm.isURI) {

                if (pom.hasRefObjectMap) {
                    val rom = pom.getRefObjectMap(0)
                    // Add non-null condition on the child reference 
                    for (jc <- rom.joinConditions)
                        conditions = conditions :+ MorphBaseQueryCondition.notNull(jc.childRef)
                } else {
                    // tp.obj is a constant IRI and there is no RefObjectMap: add an equality condition for each reference in the term map
                    val termMap = pom.objectMaps.head
                    if (termMap.isReferenceOrTemplateValued)
                        conditions = conditions ++ genEqualityConditions(termMap, tpTerm)
                }

            } else if (tpTerm.isVariable) {

                if (pom.hasRefObjectMap) {
                    val rom = pom.getRefObjectMap(0)
                    // Add non-null condition on the child reference 
                    for (jc <- rom.joinConditions)
                        conditions = conditions :+ MorphBaseQueryCondition.notNull(jc.childRef)
                } else {
                    // tp.obj is a Variable and there is no RefObjectMap: add a non-null condition for each reference in the term map
                    val termMap = pom.objectMaps.head
                    if (termMap.isReferenceOrTemplateValued)
                        conditions = conditions ++ termMap.getReferences.map(ref => MorphBaseQueryCondition.notNull(ref))
                }
            }
        }

        if (logger.isDebugEnabled()) logger.debug("Translation returns conditions: " + conditions)
        conditions
    }

    /**
     * Generate the set of conditions to match the object of a triple pattern with the subject
     * of a referencing object map.
     * A condition is either isNotNull(JSONPath expression) or equals(JSONPath expression, value)
     *
     * @param tp a SPARQL triple pattern
     * @param tm a triples map that has been assessed as a candidate triples map for the translation of tp into a query
     * @return set of conditions, either non-null or equality
     * @thrown MorphException if the triples map has no referencing object map
     */
    def genCondParent(tp: Triple, tm: R2RMLTriplesMap): List[MorphBaseQueryCondition] = {

        var conditions: List[MorphBaseQueryCondition] = List.empty

        // === Object map ===
        val tpTerm = tp.getObject
        val pom = tm.predicateObjectMaps.head

        if (!pom.hasRefObjectMap)
            throw new MorphException("Triples map " + tm + " has no parent triples map. Presumably an inccorect triple pattern binding.")

        if (tpTerm.isURI) {
            // tp.obj is a constant IRI to be matched with the subject of the parent TM:
            // add an equality condition for each reference in the subject map of the parent TM
            val rom = pom.getRefObjectMap(0)
            val parentSM = mappingDocument.getParentTriplesMap(rom).subjectMap
            if (parentSM.isReferenceOrTemplateValued)
                conditions = conditions ++ genEqualityConditions(parentSM, tpTerm)

            // Add non-null condition on the parent reference
            for (jc <- rom.joinConditions)
                conditions = conditions :+ MorphBaseQueryCondition.notNull(jc.parentRef)

        } else if (tpTerm.isVariable) {
            // tp.obj is a SPARQL variable to be matched with the subject of the parent TM
            val rom = pom.getRefObjectMap(0)
            val parentSM = mappingDocument.getParentTriplesMap(rom).subjectMap
            if (parentSM.isReferenceOrTemplateValued)
                conditions = conditions ++ parentSM.getReferences.map(ref => MorphBaseQueryCondition.notNull(ref))

            // Add non-null condition on the parent reference
            for (jc <- rom.joinConditions)
                conditions = conditions :+ MorphBaseQueryCondition.notNull(jc.parentRef)
        }

        if (logger.isDebugEnabled()) logger.debug("Translation returns Parent conditions: " + conditions)
        conditions
    }

    /**
     * Generate one equality condition between a reference in the term map (Column name, JSONPath expression...)
     * and a term of a triple pattern.
     *
     * If the term map is reference-valued, one equality condition is generated.
     * If the term map is template-value, possibly several equality conditions are generated, i.e. one for each
     * capturing group in the template string.
     *
     * Return an empty list for other types of term map.
     *
     * @param targetQuery child or parent, i.e. the query to which references of termMap relate to
     * @param termMap the term map that should possibly generate terms that match the triple pattern term tpTerm
     * @param tpTerm a term from a triple pattern
     * @return a list of one equality condition for a reference-value term map, possibly several conditions
     * for a template-valued term map
     */
    private def genEqualityConditions(termMap: R2RMLTermMap, tpTerm: Node): List[MorphBaseQueryCondition] = {

        if (termMap.isReferenceValued) {
            // Make a new equality condition between the reference in the term map and the value in the triple pattern term
            List(MorphBaseQueryCondition.equality(termMap.getOriginalValue, tpTerm.toString(false)))

        } else if (termMap.isTemplateValued) {
            // Get the references of the template string and the associated values in the triple pattern term
            val refValueCouples = TemplateUtility.getTemplateMatching(termMap.getOriginalValue, tpTerm.toString(false))

            // For each reference and associated value, build a new equality condition
            val refValueConds = refValueCouples.map(m => {
                if (termMap.inferTermType == Constants.R2RML_IRI_URI)
                    MorphBaseQueryCondition.equality(m._1, GeneralUtility.decodeURI(m._2))
                else
                    MorphBaseQueryCondition.equality(m._1, m._2)
            })
            refValueConds.toList
        } else
            List.empty
    }
}