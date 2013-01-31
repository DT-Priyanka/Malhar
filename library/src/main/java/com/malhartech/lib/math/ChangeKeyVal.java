/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseNumberKeyValueOperator;
import com.malhartech.lib.util.KeyValPair;
import java.util.HashMap;
import org.apache.commons.lang.mutable.MutableDouble;

/**
 *
 * Emits the change in the value of the key in stream on port data (as compared to a base value set via port base) for every tuple as KeyValPair. <p>
 * This is a pass through node. Tuples that arrive on base port are kept in cache forever<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>base</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>change</b>: emits KeyValPair&lt;K,V&gt;(1)<br>
 * <b>percent</b>: emits KeyValPair&lt;K,Double&gt;(1)<br>
 * <br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for ChangeMap&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>20 million k,v pairs/sec</b></td><td>Emits one key,val pair per input key,val pair per port</td>
 * <td>In-bound rate is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String, V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for ChangeMap&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th colspan=2>In-bound (<i>data</i>::process)</th><th colspan=2>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(Map&lt;K,V&gt;)</th><th><i>base</i>(Map&lt;K,V&gt;)</th><th><i>change</i>(KeyValPair&lt;K,V&gt;(1))</th><th><i>percent</i>(KeyValPair&lt;K,Double&gt;(1))</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td></td><td>{a=2,b=10,c=100}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=3,b=2,c=4}</td><td></td><td>{a=1}<br>{b=-8}<br>{c=-96}</td><td>{a=50.0}<br>{b=-80.0}<br>{c=-96.0}</td></tr>
 * <tr><td>Data (process())</td><td>{a=4,b=19,c=150}</td><td></td><td>{a=2}<br>{b=9}<br>{c=50}</td><td>{a=100.0}<br>{b=90.0}<br>{c=50.0}</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 *
 * @author Locknath Shil <locknath@malhar-inc.com><br>
 * <br>
 */
public class ChangeKeyVal<K, V extends Number> extends BaseNumberKeyValueOperator<K, V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<KeyValPair<K, V>> data = new DefaultInputPort<KeyValPair<K, V>>(this)
  {
    /**
     * Process each key, compute change or percent, and emit it.
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      K key = tuple.getKey();
      if (!doprocessKey(key)) {
        return;
      }
      MutableDouble bval = basemap.get(key);
      if (bval != null) { // Only process keys that are in the basemap
        double cval = tuple.getValue().doubleValue() - bval.doubleValue();
        change.emit(new KeyValPair<K, V>(cloneKey(key), getValue(cval)));
        percent.emit(new KeyValPair<K, Double>(cloneKey(key), (cval / bval.doubleValue()) * 100));
      }
    }
  };
  @InputPortFieldAnnotation(name = "base")
  public final transient DefaultInputPort<KeyValPair<K, V>> base = new DefaultInputPort<KeyValPair<K, V>>(this)
  {
    /**
     * Process each key to store the value. If same key appears again update with latest value.
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      if (tuple.getValue().doubleValue() != 0.0) { // Avoid divide by zero, Emit an error tuple?
        MutableDouble val = basemap.get(tuple.getKey());
        if (val == null) {
          val = new MutableDouble(0.0);
          basemap.put(cloneKey(tuple.getKey()), val);
        }
        val.setValue(tuple.getValue().doubleValue());
      }
    }
  };
  // Default partition "pass through" works for change and percent, as it is done per tuple
  @OutputPortFieldAnnotation(name = "change", optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, V>> change = new DefaultOutputPort<KeyValPair<K, V>>(this);
  @OutputPortFieldAnnotation(name = "percent", optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Double>> percent = new DefaultOutputPort<KeyValPair<K, Double>>(this);
  /**
   * basemap is a stateful field. It is retained across windows
   */
  private HashMap<K, MutableDouble> basemap = new HashMap<K, MutableDouble>();
}