/*
    Copyright 2010 Nathan Marz
 
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
 
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
 
    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package cascalog;

import cascalog.hadoop.ClojureKryoSerialization;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.log4j.Logger;

public class KryoService {

    /**
     * Initial capacity of the Kryo object buffer.
     */
    private static final int INIT_CAPACITY = 2000;

    /**
     * Maximum capacity of the Kryo object buffer.
     */
    private static final int FINAL_CAPACITY = 2000000000;

    public static final Logger LOG = Logger.getLogger(KryoService.class);

    private static Kryo freshKryo() {
        Kryo k = new ClojureKryoSerialization().populatedKryo();
        k.setRegistrationRequired(false);
        return k;
    }

    public static byte[] serialize(Object obj) {
        LOG.debug("Serializing " + obj);
        Output output = new Output(INIT_CAPACITY, FINAL_CAPACITY);
        freshKryo().writeClassAndObject(output, obj);
        return output.toBytes();
    }

    public static Object deserialize(byte[] serialized) {
        Object o = freshKryo().readClassAndObject(new Input(serialized));
        LOG.debug("Deserialized " + o);
        return o;
    }
}
