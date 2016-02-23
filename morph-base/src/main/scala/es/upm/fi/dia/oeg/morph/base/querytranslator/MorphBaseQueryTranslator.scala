package es.upm.fi.dia.oeg.morph.base.querytranslator

import org.apache.log4j.Logger
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.algebra.Algebra
import com.hp.hpl.jena.sparql.algebra.Op
import com.hp.hpl.jena.sparql.algebra.optimize.Optimize
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.TemplateUtility
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.query.MorphAbstractAtomicQuery
import es.upm.fi.dia.oeg.morph.base.query.MorphAbstractQuery
import es.upm.fi.dia.oeg.morph.base.query.MorphAbstractQueryInnerJoinRef
import es.upm.fi.dia.oeg.morph.base.query.MorphAbstractQueryUnion
import es.upm.fi.dia.oeg.morph.base.querytranslator.engine.MorphQueryRewritterFactory
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.base.query.GenericQuery

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

    Optimize.setFactory(new MorphQueryRewritterFactory());

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
    def transTPm(tp: Triple, tmSet: List[R2RMLTriplesMap]): MorphAbstractQuery = {

        val unionOf = for (tm <- tmSet) yield {

            // Sanity checks about the @NORMALIZED_ASSUMPTION
            val poms = tm.getPropertyMappings
            if (poms.isEmpty || poms.size > 1)
                throw new MorphException("The candidate triples map " + tm.toString + " must have exactly one predicate-object map.")
            val pom = poms.head
            if (pom.predicateMaps.size != 1 ||
                !((pom.objectMaps.size == 0 && pom.refObjectMaps.size == 1) || (pom.objectMaps.size == 1 && pom.refObjectMaps.size == 0)))
                throw new MorphException("The candidate triples map " + tm.toString + " must have exactly one predicate map and one object map.")

            // Start translation
            val from = tm.logicalSource
            val project = genProjection(tp, tm)
            val where = genCond(tp, tm)
            val q1 = new MorphAbstractAtomicQuery(Some(tm), from, project, where)
            val Q =
                if (!pom.hasRefObjectMap)
                    // If there is no parent triples map, simply return this atomic abstract query
                    q1
                else {
                    // If there is a parent triples map, create an INNER JOIN ON childRef = parentRef
                    val rom = pom.getRefObjectMap(0)
                    val Pfrom = mappingDocument.getParentTriplesMap(rom).logicalSource
                    val Pproject = genProjectionParent(tp, tm)
                    val Pwhere = genCondParent(tp, tm)
                    val q2 = new MorphAbstractAtomicQuery(None, Pfrom, Pproject, Pwhere) // no TM is bound to the parent query, only to the child

                    if (rom.joinConditions.size != 1)
                        logger.warn("Multiple join conditions not supported in a ReferencingObjectMap. Considering only the first one.")
                    val jc = rom.joinConditions.toIterable.head // assume only one join condition 
                    new MorphAbstractQueryInnerJoinRef(Some(tm), q1, jc.childRef, q2, jc.parentRef)
                }

            Q // yield query Q for triples map tm
        }

        // If only one triples map then we return the abstract query for that TM
        val resultQ = if (unionOf.size == 1)
            unionOf.head
        else
            // If several triples map, then we return a UNION of the abstract queries for each TM
            new MorphAbstractQueryUnion(None, unionOf)

        if (logger.isDebugEnabled())
            logger.debug("transTPm: Translation of triple pattern: [" + tp + "] with triples maps " + tmSet + ":\n" + resultQ.toString)
        resultQ
    }

    /**
     * Translate an atomic abstract query into a set of concrete queries.
     * This method is used in the MorphAbstractAtomicQuery class.
     *
     * @param atomicQ the abstract atomic query
     * @return the same atomic abstract query instance in which the targetQuery member has been
     * set with a list of concrete query strings whose results must be UNIONed
     */
    def atomicAbstractQuerytoConcrete(atomicQ: MorphAbstractAtomicQuery): List[GenericQuery]

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
                if (pom.objectMaps.isEmpty)
                    throw new MorphException("Triples map " + tm + " has no object map. Maybe due to an inccorect triples map binding.")
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
            throw new MorphException("Triples map " + tm + " has no parent triples map. Maybe due to an inccorect triples map binding.")

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
            val rom = pom.getRefObjectMap(0) // @NORMALIZED_ASSUMPTION
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
            val refValueConds = refValueCouples.map(m => MorphBaseQueryCondition.equality(m._1, m._2))
            refValueConds.toList
        } else
            List.empty
    }
}