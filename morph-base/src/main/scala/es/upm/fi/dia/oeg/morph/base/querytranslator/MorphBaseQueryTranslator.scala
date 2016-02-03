package es.upm.fi.dia.oeg.morph.base.querytranslator

import org.apache.log4j.Logger
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.algebra.Algebra
import com.hp.hpl.jena.sparql.algebra.Op
import com.hp.hpl.jena.sparql.algebra.optimize.Optimize
import es.upm.fi.dia.oeg.morph.base.GenericQuery
import es.upm.fi.dia.oeg.morph.base.querytranslator.engine.MorphQueryRewritterFactory
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.base.AbstractQuery

/**
 * Abstract class for the engine that shall translate a SPARQL query into a concrete database query
 */
abstract class MorphBaseQueryTranslator extends IQueryTranslator {

    val logger = Logger.getLogger(this.getClass());

    /** Map of nodes of a SPARQL query and the candidate triples maps for each node **/
    var mapInferredTMs: Map[Node, Set[R2RMLTriplesMap]] = Map.empty;

    Optimize.setFactory(new MorphQueryRewritterFactory());

    /**
     * High level entry point to the query translation process.
     *
     * @param sparqlQuery the SPARQL query to translate
     * @return set of concrete database queries. In the RDB case, there should be only one query. 
     * The result may be empty but not null. 
     */
    def translate(sparqlQuery: Query): AbstractQuery = {
        val start = System.currentTimeMillis()
        val result = this.translate(Algebra.compile(sparqlQuery));
        logger.info("Query translation time = " + (System.currentTimeMillis() - start) + "ms.");
        result
    }

    protected def translate(op: Op): AbstractQuery
}