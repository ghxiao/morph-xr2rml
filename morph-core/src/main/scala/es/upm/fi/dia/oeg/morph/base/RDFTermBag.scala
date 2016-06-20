package es.upm.fi.dia.oeg.morph.base

class RDFTermBag(val v: List[RDFTerm]) extends RDFTerm(v) {

    override val isBag = true

    def getMembers = v

    override def equals(a: Any): Boolean = {
        this.getClass == a.getClass && value == a.asInstanceOf[RDFTermBag].value
    }

    override def hashCode(): Int = {
        this.getClass.hashCode + value.hashCode()
    }
}