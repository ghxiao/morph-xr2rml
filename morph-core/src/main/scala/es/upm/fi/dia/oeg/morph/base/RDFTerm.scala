package es.upm.fi.dia.oeg.morph.base

class RDFTerm(val value: Object) extends java.io.Serializable {

    val isIRI = false
    val isLiteral = false
    val isBlankNode = false
    val isList = false
    val isBag = false
    val isSeq = false
    val isAlt = false

    override def toString: String = value.toString
}