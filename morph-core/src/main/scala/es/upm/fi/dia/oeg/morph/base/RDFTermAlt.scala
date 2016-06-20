package es.upm.fi.dia.oeg.morph.base

class RDFTermAlt(val v: List[RDFTerm]) extends RDFTerm(v) {

    override val isAlt = true

    def getMembers = v

    override def equals(a: Any): Boolean = {
        this.getClass == a.getClass && value == a.asInstanceOf[RDFTermAlt].value
    }

    override def hashCode(): Int = {
        this.getClass.hashCode + value.hashCode()
    }
}