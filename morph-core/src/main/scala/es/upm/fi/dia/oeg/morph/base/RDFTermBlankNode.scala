package es.upm.fi.dia.oeg.morph.base

class RDFTermBlankNode(val v: Object) extends RDFTerm(v) {

    override val isBlankNode = true

    override def equals(a: Any): Boolean = {
        this.getClass == a.getClass && value == a.asInstanceOf[RDFTermBlankNode].value
    }

    override def hashCode(): Int = {
        this.getClass.hashCode + value.hashCode()
    }
}