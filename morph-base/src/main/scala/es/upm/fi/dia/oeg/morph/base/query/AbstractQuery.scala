package es.upm.fi.dia.oeg.morph.base.query

import es.upm.fi.dia.oeg.morph.base.MorphBaseResultRdfTerms
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryOptimizer
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.base.querytranslator.TpBindings

/**
 * Representation of an abstract query as defined in https://hal.archives-ouvertes.fr/hal-01245883.
 *
 * Also used to store the concrete query (attribute targetQuery) that results from
 * the translation of this abstract query into the target database language.
 *
 * @param tpBindings a set of couples (triple pattern, List(triples map)) for which we create this atomic query.
 * This member is not empty for atomic queries only. Initially each atomic query should have exactly a binding.
 * After the abstract query optimization (e.g. self-join elimination) there may be more than one binding.
 *
 * @param limit the value of the LIMIT keyword in the SPARQL query: this is a number of solutions, not a number of triples.
 *
 * @author Franck Michel, I3S laboratory
 */
abstract class AbstractQuery(
        val tpBindings: TpBindings,
        var limit: Option[Long]) {

    // String version of the limit, to be used in toString() methods
    def limitStr = { if (limit.isDefined) ("\nLIMIT " + limit.get) else "" }

    def setLimit(lim: Option[Long]): AbstractQuery = {
        this.limit = lim
        this
    }

    /**
     * Result of translating this abstract query into a target database query.
     * This should contain a single query for an RDB.
     * For MongoDB this field is used only for atomic abstract queries, and may
     * contain several queries whose results must be UNIONed, that's why this is a list of queries
     */
    var targetQuery: List[GenericQuery] = List.empty

    def setTargetQuery(tq: List[GenericQuery]): AbstractQuery = {
        this.targetQuery = tq
        this
    }

    override def equals(a: Any): Boolean = {
        throw new MorphException("Method not implemented")
    }

    override def hashCode(): Int = {
        throw new MorphException("Method not implemented")
    }

    //---- Abstract methods ----

    /**
     *  Produce a string representation of this abstract query using the concrete (target) query string
     *  associated to each atomic query.
     */
    def toStringConcrete: String

    /**
     * Check if atomic abstract queries within this abstract query have a target query
     * properly initialized, i.e. targetQuery is not empty
     */
    def isTargetQuerySet: Boolean

    /**
     * Translate all abstract queries within this abstract query into concrete queries.
     * @param translator the query translator
     */
    def translateAbstactQueriesToConcrete(translator: MorphBaseQueryTranslator): Unit

    /**
     * Return the set of SPARQL variables projected by this abstract query.
     * This is a union of the variables projected in each atomic queries within this query.
     */
    def getVariables: Set[String]

    /**
     * Get the xR2RML projection of variable ?x in this query.
     *
     * @param varName the variable name
     * @return a set of projections in which the 'as' field is defined and equals 'varName'
     */
    def getProjectionsForVariable(varName: String): Set[AbstractQueryProjection]

    /**
     * Misc optimizations of the abstract query: self-join and self-union eliminations, propagate filters etc.
     */
    def optimizeQuery(optimizer: MorphBaseQueryOptimizer): AbstractQuery

    /**
     * Execute this query and produce the RDF terms for each of the result documents,
     * by applying the triples map bound to this query.
     *
     * @param dataSourceReader the data source reader to query the database
     * @param dataTrans the data translator to create RDF terms
     * @return a list of MorphBaseResultRdfTerms instances, one for each result document
     * May return an empty result but NOT null.
     */
    def generateRdfTerms(dataSourceReader: MorphBaseDataSourceReader, dataTranslator: MorphBaseDataTranslator): List[MorphBaseResultRdfTerms]
}

object AbstractQuery {
    /**
     * Find the list of SPARQL variables shared by two abstract queries
     */
    def getSharedVariables(left: AbstractQuery, right: AbstractQuery) = {
        left.getVariables.intersect(right.getVariables)
    }
}