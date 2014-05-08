/*
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.common.pmml;

import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.settings.InboundSettings;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.FieldUsageType;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.OpType;
import org.dmg.pmml.TypeDefinitionField;
import org.dmg.pmml.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class PMMLUtils {

  private PMMLUtils() {
  }

  /*
  public static DataDictionary buildDataDictionaryFromLevels(InboundSettings inboundSettings,
      Map<Integer, List<String>> columnToCategoryLevels) {
    Map<Integer, BiMap<String, Integer>> m = Maps.transformValues(columnToCategoryLevels,
        new Function<List<String>, BiMap<String, Integer>>() {
          @Override
          public BiMap<String, Integer> apply(List<String> input) {
            BiMap<String, Integer> r = HashBiMap.create(input.size());
            for (int i = 0; i < input.size(); i++) {
              r.put(input.get(i), i);
            }
            return r;
          }
        });
    return buildDataDictionary(inboundSettings, m);
  }
   */

  public static DataDictionary buildDataDictionary(
      InboundSettings inboundSettings,
      Map<Integer, BiMap<String,Integer>> columnToCategoryNameToIDMapping) {
    List<String> columnNames = inboundSettings.getColumnNames();
    DataDictionary dictionary = new DataDictionary();
    for (Map.Entry<Integer,BiMap<String,Integer>> entry : columnToCategoryNameToIDMapping.entrySet()) {
      int column = entry.getKey();
      String columnName = columnNames.get(column);
      DataField field = new DataField(new FieldName(columnName), OpType.CATEGORICAL, DataType.STRING);
      for (String value : entry.getValue().keySet()) {
        field.getValues().add(new Value(value));
      }
      dictionary.getDataFields().add(field);
    }
    // This won't cover numeric fields so...
    for (int numericColumn : inboundSettings.getNumericColumns()) {
      DataField field = new DataField(new FieldName(columnNames.get(numericColumn)),
          OpType.CONTINUOUS,
          DataType.DOUBLE);
      dictionary.getDataFields().add(field);
    }
    return dictionary;
  }

  public static Map<Integer, BiMap<String, Integer>> buildColumnCategoryMapping(DataDictionary dictionary) {
    Preconditions.checkNotNull(dictionary);
    InboundSettings settings = InboundSettings.create(ConfigUtils.getDefaultConfig());
    List<String> columnNames = settings.getColumnNames();

    Preconditions.checkNotNull(dictionary);
    Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping = new HashMap<>();
    for (TypeDefinitionField field : dictionary.getDataFields()) {
      Collection<Value> values = field.getValues();
      if (values != null && !values.isEmpty()) {
        String columnName = field.getName().getValue();
        int columnNumber = columnNames.indexOf(columnName);
        BiMap<String,Integer> categoryNameToID = columnToCategoryNameToIDMapping.get(columnNumber);
        if (categoryNameToID == null) {
          categoryNameToID = HashBiMap.create();
          columnToCategoryNameToIDMapping.put(columnNumber, categoryNameToID);
        }
        for (Value value : values) {
          categoryNameToID.put(value.getValue(), categoryNameToID.size());
        }
      }
    }
    return columnToCategoryNameToIDMapping;
  }

  public static MiningSchema buildMiningSchema(InboundSettings inboundSettings) {
    return buildMiningSchema(inboundSettings, null);
  }

  public static MiningSchema buildMiningSchema(InboundSettings inboundSettings, double[] importances) {
    List<String> columnNames = inboundSettings.getColumnNames();
    Integer targetColumn = inboundSettings.getTargetColumn();
    Collection<Integer> ignored = inboundSettings.getIgnoredColumns();
    Collection<Integer> ids = inboundSettings.getIdColumns();
    Collection<Integer> numeric = inboundSettings.getNumericColumns();
    Collection<Integer> categorical = inboundSettings.getCategoricalColumns();
    Collection<MiningField> miningFields = new ArrayList<>();
    for (int col = 0; col < columnNames.size(); col++) {
      if (ignored.contains(col) || ids.contains(col)) {
        continue;
      }
      MiningField field = new MiningField(new FieldName(columnNames.get(col)));
      if (numeric.contains(col)) {
        field.setOptype(OpType.CONTINUOUS);
      } else if (categorical.contains(col)) {
        field.setOptype(OpType.CATEGORICAL);
      } else {
        throw new IllegalStateException("No type for col " + col);
      }
      if (targetColumn != null && col == targetColumn) {
        field.setUsageType(FieldUsageType.PREDICTED);
      } else {
        if (importances != null) {
          field.setImportance(importances[col]);
        }
      }
      miningFields.add(field);
    }
    return new MiningSchema().withMiningFields(miningFields);
  }
}
