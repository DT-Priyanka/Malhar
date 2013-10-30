package com.datatorrent.contrib.machinedata.operator;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.contrib.machinedata.data.AverageData;
import com.datatorrent.contrib.machinedata.data.MachineKey;
import com.datatorrent.lib.util.KeyHashValPair;


public class MachineInfoAveragingUnifier implements Unifier<KeyHashValPair<MachineKey, Map<String, AverageData>>>
{

  private Map<MachineKey, Map<String, AverageData>> sums = new HashMap<MachineKey, Map<String, AverageData>>();
  public final transient DefaultOutputPort<KeyHashValPair<MachineKey, Map<String, AverageData>>> outputPort = new DefaultOutputPort<KeyHashValPair<MachineKey, Map<String, AverageData>>>();

  @Override
  public void beginWindow(long arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void endWindow()
  {
    for (Map.Entry<MachineKey, Map<String, AverageData>> entry : sums.entrySet()) {
      outputPort.emit(new KeyHashValPair<MachineKey, Map<String, AverageData>>(entry.getKey(), entry.getValue()));
    }
    sums.clear();
  
  }

  @Override
  public void setup(OperatorContext arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void process(KeyHashValPair<MachineKey, Map<String, AverageData>> arg0)
  {
    MachineKey tupleKey = arg0.getKey();
    Map<String, AverageData> sumsMap = sums.get(tupleKey);
    Map<String, AverageData> tupleValue = arg0.getValue();
    if (sumsMap == null) {
      sums.put(tupleKey, tupleValue);
    } else {
      updateDate("cpu", sumsMap, tupleValue);
      updateDate("ram", sumsMap, tupleValue);
      updateDate("hdd", sumsMap, tupleValue);
    }

  }

  private void updateDate(String key, Map<String, AverageData> sumsMap, Map<String, AverageData> tupleMap)
  {
    AverageData sumsAverageData = sumsMap.get(key);
    AverageData tupleAverageData = tupleMap.get(key);
    if (tupleAverageData != null) {
      if (sumsAverageData != null) {
        sumsAverageData.setCount(sumsAverageData.getCount() + tupleAverageData.getCount());
        sumsAverageData.setSum(sumsAverageData.getSum() + tupleAverageData.getSum());
      } else {
        sumsMap.put(key, tupleAverageData);
      }
    }

  }

}