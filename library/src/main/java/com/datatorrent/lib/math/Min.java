/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.math;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseNumberValueOperator;

/**
 * Emits at end of window minimum of all values sub-classed from Number in the incoming stream. <br>
 * <br>
 * <b>StateFull :</b>Yes, min value is computed over application windows. <br>
 * <b>Partitions :</b>Yes, operator is kin unifier operator. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends Number<br>
 * <b>min</b>: emits V extends Number<br>
 * <br>
 * <br>
 *
 * @since 0.3.2
 */
public class Min<V extends Number> extends BaseNumberValueOperator<V> implements Unifier<V>
{
	/**
	 * Computed low value.
	 */
  protected V low;
  
  // transient field
  protected boolean flag = false;
  
	/**
	 * Input port.
	 */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>()
  {
    /**
     * Each tuple is compared to the min and a new min (if so) is stored.
     */
    @Override
    public void process(V tuple)
    {
      Min.this.process(tuple);
    }
  };

  /**
   * Unifier process function.
   */
  @Override
  public void process(V tuple)
  {
    if (!flag) {
      low = tuple;
      flag = true;
    }
    else if (low.doubleValue() > tuple.doubleValue()) {
      low = tuple;
    }
  }

  /**
   * Min output port.
   */
  @OutputPortFieldAnnotation(name = "min")
  public final transient DefaultOutputPort<V> min = new DefaultOutputPort<V>()
  {
    @Override
    public Unifier<V> getUnifier()
    {
      return Min.this;
    }
  };

  /**
   * Emits the max. Override getValue if tuple type is mutable.
   * Clears internal data. Node only works in windowed mode.
   */
  @Override
  public void endWindow()
  {
    if (flag) {
      min.emit(low);
    }
    flag = false;
    low = null;
  }
}
