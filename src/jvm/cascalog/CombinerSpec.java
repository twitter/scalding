/*
    Copyright 2010 Nathan Marz
 
    Project and contact information: http://www.cascalog.org/ 

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
   
        http://www.apache.org/licenses/LICENSE-2.0
   
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package cascalog;

import java.io.Serializable;

public class CombinerSpec implements Serializable {
    public Object[] init_spec;
    public Object[] combiner_spec;
    public Object[] extractor_spec;

    public CombinerSpec(Object[] init_spec, Object[] combiner_spec) {
        this(init_spec, combiner_spec, null);
    }

    public CombinerSpec(Object[] init_spec, Object[] combiner_spec, Object[] extractor_spec) {
        this.init_spec = init_spec;
        this.combiner_spec = combiner_spec;
        this.extractor_spec = extractor_spec;
    }
}
