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

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;

/** This is for helping with running hadoop jobs from within emacs repl. */
public class WriterOutputStream extends OutputStream {

    private final Writer writer;

    public WriterOutputStream(Writer writer) {
        this.writer = writer;
    }

    public void write(int b) throws IOException {
        write(new byte[]{(byte) b}, 0, 1);
    }

    public void write(byte b[], int off, int len) throws IOException {
        writer.write(new String(b, off, len));
    }

    public void flush() throws IOException {
        writer.flush();
    }

    public void close() throws IOException {
        writer.close();
    }
}  
