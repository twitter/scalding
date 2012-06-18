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

import cascading.flow.FlowProcess;
import cascading.operation.AggregatorCall;
import cascading.operation.OperationCall;
import java.io.Serializable;

public abstract class CascalogAggregator implements Serializable {

    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    }

    public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
    }

    public abstract void start(FlowProcess flowProcess, AggregatorCall aggregatorCall);
    public abstract void aggregate(FlowProcess fp, AggregatorCall ac);
    public abstract void complete(FlowProcess flowProcess, AggregatorCall aggregatorCall);
}
