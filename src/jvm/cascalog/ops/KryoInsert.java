package cascalog.ops;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascalog.KryoService;

public class KryoInsert extends BaseOperation implements Function {
    /** Field values */
    private final byte[] serialized;
    private transient Tuple values;

    public KryoInsert(Fields fieldDeclaration, Object... values) {
        super( 0, fieldDeclaration );
        this.serialized = KryoService.serialize(values);

        if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != values.length )
            throw new IllegalArgumentException( "fieldDeclaration must be the same size as the given values" );
    }

    public Tuple getTuple() {
        if (this.values == null) {
            Object[] values = (Object[]) KryoService.deserialize(this.serialized);
            this.values = new Tuple(values);
        }
        return this.values;
    }

    @Override public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        functionCall.getOutputCollector().add( new Tuple( getTuple() ) );
    }

    @Override public boolean equals( Object object ) {
        if( this == object )
            return true;
        if( !( object instanceof KryoInsert ) )
            return false;
        if( !super.equals( object ) )
            return false;

        KryoInsert insert = (KryoInsert) object;

        Tuple tuple = getTuple();

        return !(tuple != null ? !tuple.equals(insert.getTuple()) : insert.getTuple() != null);
    }

    @Override public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + ( getTuple() != null ? getTuple().hashCode() : 0 );
        return result;
    }
}
