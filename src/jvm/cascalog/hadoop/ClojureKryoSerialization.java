package cascalog.hadoop;

import cascading.kryo.KryoSerialization;
import com.esotericsoftware.kryo.Kryo;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static carbonite.JavaBridge.enhanceRegistry;

/** User: sritchie Date: 12/1/11 Time: 12:21 PM */
public class ClojureKryoSerialization extends KryoSerialization {

    public ClojureKryoSerialization() {
        super();
    }

    public ClojureKryoSerialization(Configuration conf) {
        super(conf);
    }

    public Kryo decorateKryo(Kryo k) {
        try {
            enhanceRegistry(k);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        k.register(ArrayList.class);
        k.register(HashMap.class);
        k.register(HashSet.class);
        return k;
    }
}
