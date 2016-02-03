package fr.unice.i3s.morph.xr2rml.mongo.querytranslator

import org.apache.log4j.Logger
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.sparql.algebra.Op
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GenericQuery
import es.upm.fi.dia.oeg.morph.base.TemplateUtility
import es.upm.fi.dia.oeg.morph.base.UnionOfGenericQueries
import es.upm.fi.dia.oeg.morph.base.querytranslator.ConditionType
import es.upm.fi.dia.oeg.morph.base.querytranslator.IQueryCondition
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryCondition
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryConditionJoin
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.base.querytranslator.TargetQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import fr.unice.i3s.morph.xr2rml.mongo.MongoDBQuery
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNode
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCond
import com.hp.hpl.jena.sparql.algebra.op.OpUnion
import com.hp.hpl.jena.sparql.algebra.op.OpFilter
import com.hp.hpl.jena.sparql.algebra.op.OpBGP
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct
import com.hp.hpl.jena.sparql.algebra.op.OpSlice
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin
import com.hp.hpl.jena.sparql.algebra.op.OpOrder
import com.hp.hpl.jena.sparql.algebra.op.OpJoin
import com.hp.hpl.jena.sparql.algebra.op.OpGroup
import com.hp.hpl.jena.sparql.algebra.op.OpExtend
import com.hp.hpl.jena.sparql.algebra.op.OpProject

/**
 * This class assumes that the xR2RML mapping is normalized, that is, a triples map
 * has not more that one predicate-object map, and each predicate-object map has
 * exactly one predicate and one object map.
 * In the code this assumption is mentioned by the annotation @NORMALIZED_ASSUMPTION
 */
class MorphMongoQueryTranslator(val md: R2RMLMappingDocument) extends MorphBaseQueryTranslator {

    override val logger = Logger.getLogger(this.getClass());

    /**
     * High level method to start the translation process
     *
     * @return a instance of UnionOfGenericQueries, a catch-all class providing the child query, the parent
     * query and join conditions if they have to be performed by the xR2RML engine (when the database does
     * not support join).
     *
     */
    override def translate(op: Op): UnionOfGenericQueries = {
        if (logger.isDebugEnabled()) logger.debug("opSparqlQuery = " + op)

        // WARNING ####################################################################
        // This is totally adhoc code meant to test the whole process of running Morph-xR2RML with
        // a query of one triple pattern
        val tmMovies = md.getClassMappingsByName("Movies")
        val tmDirectors = md.getClassMappingsByName("Directors")

        // Do the translation of the first triple pattern
        val bgp = op.asInstanceOf[OpProject].getSubOp().asInstanceOf[OpBGP]
        val triples = bgp.getPattern().getList()
        this.transTP(triples.get(0), tmMovies)
        // ####################################################################
    }

    /**
     * Translation of a triple pattern into a union of queries based on a candidate triples map.
     *
     * @param tp a SPARQL triple pattern
     * @param tm a triples map that has been assessed as a candidate triples map for tp, that is a
     * triples map that should potentially be able to generate triples matching tp
     * @return a instance of UnionOfGenericQueries, a catch-all class providing the child query, the parent
     * query and join conditions if they have to be performed by the xR2RML engine (when the database does
     * not support join).
     */
    def transTP(tp: Triple, tm: R2RMLTriplesMap): UnionOfGenericQueries = {

        // Sanity checks about the @NORMALIZED_ASSUMPTION
        val poms = tm.getPropertyMappings
        if (poms.isEmpty || poms.size > 1) {
            logger.error("The candidate triples map " + tm.toString + " must have exactly one predicate-object map.")
            return UnionOfGenericQueries(List.empty)
        }

        val pom = poms.head
        if (pom.predicateMaps.size != 1 &&
            ((pom.objectMaps.size == 0 && pom.refObjectMaps.size == 1) || (pom.objectMaps.size == 1 && pom.refObjectMaps.size == 0))) {
            logger.error("The candidate triples map " + tm.toString + " must have exactly one predicate map and one object map.")
            return UnionOfGenericQueries(List.empty)
        }

        val logSrc = tm.getLogicalSource
        if (logSrc.logicalTableType != Constants.LogicalTableType.QUERY) {
            logger.error("Logical source table type is not compatible with MongoDB.")
            return UnionOfGenericQueries(List.empty)
        }

        if (logger.isDebugEnabled()) logger.debug("Translation of triple pattern: [" + tp + "] with triples map " + tm)

        // Start translation
        val fromPart = genFrom(tm)
        val selectPart = genProjection(tp, tm)
        val wherePart = genCond(tp, tm)

        if (logger.isDebugEnabled())
            logger.debug("transTP(" + tp + ", " + tm + "):\n" +
                "fromPart:  " + fromPart + "\n" +
                "selecPart: " + selectPart + "\n" +
                "wherePart: " + wherePart)

        // Select all NotNull and Equality conditions (join conditions will be processed by the processor later on)
        val pushDownConds = wherePart.filterNot(_.condType == ConditionType.Join).map(c => c.asInstanceOf[MorphBaseQueryCondition])

        // Split conditions depending on which query they apply to, and create the equivalent MongoDB query
        val childConds = toConcreteQuery(TargetQuery.Child, fromPart, pushDownConds)
        val parentConds = toConcreteQuery(TargetQuery.Parent, fromPart, pushDownConds)

        // Keep the join conditions separately since they shall be computed by the xR2RML engine itself  
        val joinConds = wherePart.filter(_.condType == ConditionType.Join)

        new UnionOfGenericQueries(
            childConds.map(c => new GenericQuery(Constants.DatabaseType.MongoDB, c)),
            parentConds.map(c => new GenericQuery(Constants.DatabaseType.MongoDB, c)),
            joinConds)
    }

    /**
     * Generate the data sources from the triples map: that includes at least the query from the
     * logical source of the triples map passed as parameter (the child source), and optionally
     * the query of the logical source of the parent triples map in case there is a referencing object map.
     *
     * @param tm a triples map that has been assessed as a candidate triples map for the translation of tp into a query
     * @return a map with the child query, respectively the parent query, with map keys
     * TargetQuery.Child, respectively TargetQuery.Parent. Each query itself is an instance of MongoDBQuery.
     */
    def genFrom(tm: R2RMLTriplesMap): Map[TargetQuery.Value, MongoDBQuery] = {

        val logSrc = tm.getLogicalSource
        val childQuery = MongoDBQuery.parseQueryString(logSrc.getValue, logSrc.docIterator, true)
        val pom = tm.getPropertyMappings.head // @NORMALIZED_ASSUMPTION

        val result =
            if (pom.hasRefObjectMap) {
                val rom = pom.getRefObjectMap(0) // @NORMALIZED_ASSUMPTION
                val parentTMLogSrc = md.getParentTriplesMap(rom).getLogicalSource
                val parentQuery = MongoDBQuery.parseQueryString(parentTMLogSrc.getValue, parentTMLogSrc.docIterator, true)
                Map(TargetQuery.Child -> childQuery, TargetQuery.Parent -> parentQuery)
            } else
                Map(TargetQuery.Child -> childQuery)

        if (logger.isDebugEnabled()) logger.debug("From part: " + result)
        result
    }

    /**
     * Generate the list of xR2RML references that are evaluated when generating the triples that match tp.
     * Those references are used to select the data elements to mention in the projection part of the query.
     * This function is database-dependent:
     * in the case of MongoDB, references are JSONPath expressions, from which we can figure out which
     * fields to mention in the project part of the MongoDB query.
     *
     * This helps return only needed fields from JSON documents instead of complete documents.
     * Since MongoDB does not support joins, the join shall be processed by the xR2RML engine,
     * thus joined expressions must be projected too.
     * In addition, for each variable of tp, the corresponding term map must be projected.
     *
     * @param tp a SPARQL triple pattern
     * @param tm a triples map that has been assessed as a candidate triples map for the translation of tp into a query
     * @return list of couples (Child, expression) or (Parent, expression),
     * where expression is a JSONPath reference from which named fields must be projected
     */
    def genProjection(tp: Triple, tm: R2RMLTriplesMap): List[(TargetQuery.Value, String)] = {

        var listRefs: List[(TargetQuery.Value, String)] = List.empty

        if (tp.getSubject().isVariable())
            listRefs = tm.subjectMap.getReferences.map(m => (TargetQuery.Child, m))

        val pom = tm.getPropertyMappings.head // @NORMALIZED_ASSUMPTION
        if (tp.getPredicate().isVariable()) {
            listRefs = listRefs ::: pom.predicateMaps.head.getReferences.map(m => (TargetQuery.Child, m)) // @NORMALIZED_ASSUMPTION
        }

        if (pom.hasRefObjectMap) {
            // The joined fields must always be projected, whether tp.obj is an IRI or a variable: since MongoDB
            // cannot compute joins, the xR2RML processor has to do it, thus joined fields must be returned by both queries.
            val rom = pom.getRefObjectMap(0) // @NORMALIZED_ASSUMPTION

            for (jc <- rom.joinConditions) {
                listRefs = listRefs :+ (TargetQuery.Child, jc.childRef) :+ (TargetQuery.Parent, jc.parentRef)
            }

            // In addition, if tp.obj is a variable, the subject of the parent TM must be projected too.
            if (tp.getObject().isVariable()) {
                val parentTM = md.getParentTriplesMap(rom)
                listRefs = listRefs ++ parentTM.subjectMap.getReferences.map(m => (TargetQuery.Parent, m)) // @NORMALIZED_ASSUMPTION
            }

        } else if (tp.getObject().isVariable())
            listRefs = listRefs ++ pom.objectMaps.head.getReferences.map(m => (TargetQuery.Child, m)) // @NORMALIZED_ASSUMPTION

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
     * A conditions is either isNotNull(JSONPath expression) or equals(JSONPath expression, value)
     *
     * @param tp a SPARQL triple pattern
     * @param tm a triples map that has been assessed as a candidate triples map for the translation of tp into a query
     * @return set of conditions, either not-null, equality, or join in case of RefObjectMap.
     */
    def genCond(tp: Triple, tm: R2RMLTriplesMap): List[IQueryCondition] = {

        var conditions: List[IQueryCondition] = List.empty

        { // === Subject map ===
            val tpTerm = tp.getSubject
            val termMap = tm.subjectMap

            if (termMap.isReferenceOrTemplateValued)
                if (tpTerm.isVariable)
                    // Add a not-null condition for each reference in the term map
                    conditions = conditions ++ termMap.getReferences.map(ref => MorphBaseQueryCondition.notNull(TargetQuery.Child, ref))
                else if (tpTerm.isURI) {
                    // Add an equality condition for each reference in the term map
                    conditions = conditions ++ genEqualityConditions(TargetQuery.Child, termMap, tpTerm)
                }
        }
        { // === Predicate map ===
            val tpTerm = tp.getPredicate
            val termMap = tm.predicateObjectMaps.head.predicateMaps.head // @NORMALIZED_ASSUMPTION

            if (termMap.isReferenceOrTemplateValued)
                if (tpTerm.isVariable)
                    // Add a not-null condition for each reference in the term map
                    conditions = conditions ++ termMap.getReferences.map(ref => MorphBaseQueryCondition.notNull(TargetQuery.Child, ref))
                else if (tpTerm.isURI)
                    // Add an equality condition for each reference in the term map
                    conditions = conditions ++ genEqualityConditions(TargetQuery.Child, termMap, tpTerm)
        }
        { // === Object map ===
            val tpTerm = tp.getObject
            val pom = tm.predicateObjectMaps.head // @NORMALIZED_ASSUMPTION

            if (tpTerm.isLiteral) {
                // Add an equality condition for each reference in the term map
                if (!pom.objectMaps.isEmpty) { // if there is no ObjectMap then the triples map does not really match tp. Earlier error?
                    val termMap = pom.objectMaps.head // @NORMALIZED_ASSUMPTION
                    if (termMap.isReferenceOrTemplateValued)
                        conditions = conditions ++ genEqualityConditions(TargetQuery.Child, termMap, tpTerm)
                }
            } else if (tpTerm.isURI) {

                if (pom.hasRefObjectMap) {
                    val rom = pom.getRefObjectMap(0) // @NORMALIZED_ASSUMPTION

                    // tp.obj is a constant IRI to be matched with the subject of the parent TM:
                    // add an equality condition for each reference in the subject map of the parent TM
                    val parentSM = md.getParentTriplesMap(rom).subjectMap
                    if (parentSM.isReferenceOrTemplateValued)
                        conditions = conditions ++ genEqualityConditions(TargetQuery.Parent, parentSM, tpTerm)

                    // Add the join conditions
                    for (jc <- rom.joinConditions)
                        conditions = conditions :+ new MorphBaseQueryConditionJoin(
                            jc.childRef, tm.logicalSource.docIterator,
                            jc.parentRef, md.getParentTriplesMap(rom).logicalSource.docIterator)
                } else {
                    // tp.obj is a constant IRI and there is no RefObjectMap: add an equality condition for each reference in the term map
                    if (!pom.objectMaps.isEmpty) { // if there is no ObjectMap then the triples map does not really match tp. Earlier error?
                        val termMap = pom.objectMaps.head // @NORMALIZED_ASSUMPTION
                        if (termMap.isReferenceOrTemplateValued)
                            conditions = conditions ++ genEqualityConditions(TargetQuery.Child, termMap, tpTerm)
                    }
                }

            } else if (tpTerm.isVariable) {

                if (pom.hasRefObjectMap) {
                    val rom = pom.getRefObjectMap(0) // @NORMALIZED_ASSUMPTION

                    // tp.obj is a SPARQL variable to be matched with the subject of the parent TM
                    val parentSM = md.getParentTriplesMap(rom).subjectMap
                    if (parentSM.isReferenceOrTemplateValued)
                        conditions = conditions ++ parentSM.getReferences.map(ref => MorphBaseQueryCondition.notNull(TargetQuery.Parent, ref))

                    // Add the join conditions
                    for (jc <- rom.joinConditions)
                        conditions = conditions :+ new MorphBaseQueryConditionJoin(
                            jc.childRef, tm.logicalSource.docIterator,
                            jc.parentRef, md.getParentTriplesMap(rom).logicalSource.docIterator)
                } else {
                    // tp.obj is a SPARQL variable with no RefObjectMap: simply add a not-null condition for each reference in the term map
                    val termMap = pom.objectMaps.head // @NORMALIZED_ASSUMPTION
                    if (termMap.isReferenceOrTemplateValued)
                        conditions = conditions ++ termMap.getReferences.map(ref => MorphBaseQueryCondition.notNull(TargetQuery.Child, ref))
                }
            }
        }

        if (logger.isDebugEnabled()) logger.debug("Translation returns conditions: " + conditions)
        conditions
    }

    /**
     * Generate one equality condition between a reference in the term map (Column name, JSONPath expression...)
     * and a term of a triple pattern.<br>
     * If the term map is reference-valued, one equality condition is generated.<br>
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
    private def genEqualityConditions(targetQuery: TargetQuery.Value, termMap: R2RMLTermMap, tpTerm: Node): List[MorphBaseQueryCondition] = {

        if (termMap.isReferenceValued) {
            // Make a new equality condition between the reference in the term map and the value in the triple pattern term
            List(MorphBaseQueryCondition.equality(targetQuery, termMap.getOriginalValue, tpTerm.toString(false)))

        } else if (termMap.isTemplateValued) {
            // Get the references of the template string and the associated values in the triple pattern term
            val refValueCouples = TemplateUtility.getTemplateMatching(termMap.getOriginalValue, tpTerm.toString(false))

            // For each reference and associated value, build a new equality condition
            val refValueConds = refValueCouples.map(m => MorphBaseQueryCondition.equality(targetQuery, m._1, m._2))
            refValueConds.toList
        } else
            List.empty
    }

    /**
     * Translate as set of NotNull or Equality conditions into concrete MongoDB queries.
     *
     * @param targetQuery the query on which the conditions apply, either child or parent.
     * @param fromPart the result of the genFrom method: a map of the mandatory child and optional parent queries.
     * This is necessary to know the iterator to prepend to references in the conditions, and to add the original
     * query that is provided in the triples map logical source.
     * @param pushDownConds the actual set of NotNull or Equality conditions to translate into concrete MongoDB queries.
     * @return a list of concrete query strings, to be executed separately and UNIONed by the xR2RML engine.
     */
    private def toConcreteQuery(
        targetQuery: TargetQuery.Value,
        fromPart: Map[TargetQuery.Value, MongoDBQuery],
        pushDownConds: List[MorphBaseQueryCondition]): List[String] = {

        if (!fromPart.contains(targetQuery) || pushDownConds.isEmpty)
            return List.empty

        // Split conditions depending on which query they apply to, and generate the concrete query for each one
        val conds = pushDownConds.filter(_.targetQuery == targetQuery).map(cond => {

            // If there is an iterator, replace the starting "$" of the JSONPath reference with the iterator path
            val iter = fromPart(targetQuery).iterator
            val condRefIter =
                if (iter.isDefined)
                    cond.reference.replace("$", iter.get)
                else
                    cond.reference

            // Run the translation into a concrete MongoDB query
            if (cond.condType == ConditionType.IsNotNull)
                JsonPathToMongoTranslator.trans(condRefIter, new MongoQueryNodeCond(ConditionType.IsNotNull, null), true)
            else // Equality condition
                JsonPathToMongoTranslator.trans(condRefIter, new MongoQueryNodeCond(ConditionType.Equals, cond.eqValue), true)
        })

        if (logger.isTraceEnabled())
            logger.debug("Condtion set for " + targetQuery + " query was translated to: [" + conds + "]")

        // Merge queries with a common root
        val fused = MongoQueryNode.fusionQueries(conds)
        val listFused =
            if (fromPart(targetQuery).query.isEmpty())
                fused.map(q => q.toString)
            else
                fromPart(targetQuery).query +: fused.map(q => q.toString)

        listFused
    }
}