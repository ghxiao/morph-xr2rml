package es.upm.fi.dia.oeg.morph.base.query

import org.apache.log4j.Logger

import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryOptimizer
import es.upm.fi.dia.oeg.morph.base.querytranslator.TPBinding
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource

/**
 * Representation of the abstract atomic query as defined in https://hal.archives-ouvertes.fr/hal-01245883
 *
 * @param tpBindings a couple (triple pattern, triples map) for which we create this atomic query.
 * Empty in the case of a child or parent query in a referencing object map, and in this case the binding is
 * in the instance of AbstractQueryInnerJoinRef.
 * tpBindings may contain several bindings after query optimization e.g. self-join elimination i.e. 2 atomic queries are merged
 * into a single one that will be used to generate triples for by 2 triples maps i.e. 2 bindings
 *
 * @param from the logical source, which must be the same as in the triples map of tpBindings
 *
 * @param project set of xR2RML references that shall be projected in the target query, i.e. the references
 * needed to generate the RDF terms of the result triples.<br>
 * Note that the same variable can be projected several times, e.g. when the same variable is used
 * several times in a triple pattern e.g.: "?x ns:predicate ?x ."<br>
 * Besides a projection can contain several references in case the variable is matched with a template
 * term map, e.g. if ?x is matched with "http://domain/{\$.ref1}/{\$.ref2.*}", then the projection is:
 * "Set(\$.ref1, \$.ref2.*) AS ?x" => in that case, we must have the same projection in the second query
 * for the merge to be possible.<br>
 * Therefore projection is a set of sets, in the most complex case we may have something like:
 * Set(Set(\$.ref1, \$.ref2.*) AS ?x, Set(\$.ref3, \$.ref4) AS ?x), Set(\$.ref5, \$.ref6) AS ?x))
 *
 * @param where set of conditions applied to xR2RML references, entailed by matching the triples map
 * with the triple pattern. If there are more than one condition the semantics is a logical AND of all.
 *
 * @param lim the value of the optional LIMIT keyword in the SPARQL graph pattern
 *
 * @author Franck Michel, I3S laboratory
 */
abstract class AbstractAtomicQuery(

    tpBindings: Set[TPBinding],
    val from: xR2RMLLogicalSource,
    val project: Set[AbstractQueryProjection],
    val where: Set[AbstractQueryCondition],
    lim: Option[Long])

        extends AbstractQuery(tpBindings, lim) {

    val logger = Logger.getLogger(this.getClass().getName())

    override def equals(a: Any): Boolean = {
        a.isInstanceOf[AbstractAtomicQuery] && {
            val p = a.asInstanceOf[AbstractAtomicQuery]
            this.from == p.from && this.project == p.project && this.where == p.where
        }
    }

    override def toString = {
        val fromStr =
            if (from.docIterator.isDefined)
                from.getValue + ", Iterator: " + from.docIterator
            else
                from.getValue

        val bdgs = if (tpBindings.nonEmpty) tpBindings.mkString(" ", ", ", "\n  ") else " "

        "{" + bdgs +
            "from   : " + fromStr + "\n" +
            "  project: " + project + "\n" +
            "  where  : " + where + " }" + limitStr
    }

    override def toStringConcrete = {
        val bdgs = if (tpBindings.nonEmpty) tpBindings.mkString(", ") + "\n " else ""
        "{ " + bdgs +
            targetQuery.map(_.concreteQuery).mkString("\nUNION\n") + " }" + limitStr
    }

    /**
     * Check if this atomic abstract queries has a target query properly initialized
     * i.e. targetQuery is not empty
     */
    override def isTargetQuerySet: Boolean = { !targetQuery.isEmpty }

    /**
     * Return the set of SPARQL variables projected in this abstract query
     */
    override def getVariables: Set[String] = {
        project.filter(_.as.isDefined).map(_.as.get)
    }

    /**
     * Get the xR2RML projection of variable ?x in this query.
     *
     * @note When a variable ?x is matched with a term map with template string
     * "http://domain/{\$.ref1}/{\$.ref2.*}", then we say that
     * Set($.ref1, $.ref2) is projected as ?x.<br>
     * In addition a variable can be projected several times in the same query, when
     * it appears several times in a triple pattern like "?x :prop ?x".<br>
     * Therefore the result is a Set of projections of x, each containing a Set of references
     *
     * @param varName the variable name
     * @return a set of projections in which the 'as' field is defined and equals 'varName'
     */
    override def getProjectionsForVariable(varName: String): Set[AbstractQueryProjection] = {
        this.project.filter(p => { p.as.isDefined && p.as.get == varName })
    }

    /**
     * Get the variables projected in this query with a given projection
     *
     * @param proj the projection given as a set of xR2RML references (this is the 'references'
     * field of an instance of AbstractQueryProjection)
     * @return a set of variable names projected as 'proj'
     */
    private def getVariablesForProjection(proj: Set[String]): Set[String] = {
        this.project.filter(p => { p.references == proj && p.as.isDefined }).map(_.as.get)
    }

    /**
     * An atomic query cannot be optimized. Return self
     */
    override def optimizeQuery(optimizer: MorphBaseQueryOptimizer): AbstractQuery = { this }
}
