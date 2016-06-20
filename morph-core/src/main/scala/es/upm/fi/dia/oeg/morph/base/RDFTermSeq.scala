package es.upm.fi.dia.oeg.morph.base

class RDFTermSeq(val v: List[RDFTerm]) extends RDFTerm(v) {

    override val isSeq = true

    def getMembers = v

    override def equals(a: Any): Boolean = {
        this.getClass == a.getClass && value == a.asInstanceOf[RDFTermSeq].value
    }

    override def hashCode(): Int = {
        this.getClass.hashCode + value.hashCode()
    }
}