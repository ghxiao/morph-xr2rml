package es.upm.fi.dia.oeg.morph.base.querytranslator

/**
 * Representation of one or several xR2RML references to be projected out of a target database query.
 * In the Project part of the abstract query representation this stands for: <reference> AS ?variable
 *
 * @param references The references on which this projection applies, typically a column name (RDBB)
 * or JSONPath expression (MongoDB) from a xrr:reference. Possibly multiple expressions in case of a rr:template property
 * @param as name of the SPARQL variable this reference stands for. Optional since a projection for a joined reference
 * does not stand for any variable
 *
 * @author Franck Michel (franck.michel@cnrs.fr)
 */
class MorphBaseQueryProjection(
        val references: Set[String],
        val as: Option[String]) {

    /** Constructor with a single reference and no AS */
    def this(reference: String) = { this(Set(reference), None) }

    override def toString: String = {
        if (as.isDefined)
            references.toString + " AS " + as.get
        else references.toString
    }

    override def equals(a: Any): Boolean = {
        a.isInstanceOf[MorphBaseQueryProjection] && {
            val p = a.asInstanceOf[MorphBaseQueryProjection]
            this.references == p.references && this.as == p.as
        }
    }
}