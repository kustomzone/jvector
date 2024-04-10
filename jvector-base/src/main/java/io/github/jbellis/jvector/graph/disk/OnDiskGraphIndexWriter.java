/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.jbellis.jvector.graph.disk;

import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.agrona.collections.Int2IntHashMap;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Map;

/**
 * Write a graph index to disk, for later loading as an OnDiskGraphIndex.
 */
public class OnDiskGraphIndexWriter {
    private static final VectorTypeSupport vectorTypeSupport = VectorizationProvider.getInstance().getVectorTypeSupport();
    private final GraphIndex graph;
    private final Map<Integer, Integer> oldToNewOrdinals;
    private EnumMap<FeatureId, FeatureWriter> features;

    private OnDiskGraphIndexWriter(GraphIndex graph, Map<Integer, Integer> oldToNewOrdinals, EnumMap<FeatureId, FeatureWriter> features) {
        this.graph = graph;
        this.oldToNewOrdinals = oldToNewOrdinals;
        this.features = features;
    }

    public void write(DataOutput out) throws IOException
    {
        if (graph instanceof OnHeapGraphIndex) {
            var ohgi = (OnHeapGraphIndex) graph;
            if (ohgi.getDeletedNodes().cardinality() > 0) {
                throw new IllegalArgumentException("Run builder.cleanup() before writing the graph");
            }
        }
        if (oldToNewOrdinals.size() != graph.size()) {
            throw new IllegalArgumentException(String.format("ordinalMapper size %d does not match graph size %d",
                    oldToNewOrdinals.size(), graph.size()));
        }

        var entriesByNewOrdinal = new ArrayList<>(oldToNewOrdinals.entrySet());
        entriesByNewOrdinal.sort(Comparator.comparingInt(Map.Entry::getValue));
        // the last new ordinal should be size-1
        if (graph.size() > 0 && entriesByNewOrdinal.get(entriesByNewOrdinal.size() - 1).getValue() != graph.size() - 1) {
            throw new IllegalArgumentException("oldToNewOrdinals produced out-of-range entries");
        }

        int dimension = 0;
        if (features.containsKey(FeatureId.INLINE_VECTORS)) {
            dimension = features.get(FeatureId.INLINE_VECTORS).inlineSize() / Float.BYTES;
        } else if (features.containsKey(FeatureId.LVQ)) {
            dimension = features.get(FeatureId.LVQ).inlineSize();
        }

        try (var view = graph.getView()) {
            // graph-level properties
            var commonHeader = new CommonHeader(OnDiskGraphIndex.CURRENT_VERSION, graph.size(), dimension, view.entryNode(), graph.maxDegree());
            var header = new Header(commonHeader, features);
            header.write(out);

            var orderedFeatures = features.values().stream().sorted(Comparator.comparingInt(f -> f.id().bitshift())).toArray(FeatureWriter[]::new);

            // for each graph node, write the associated vector and its neighbors
            for (int i = 0; i < oldToNewOrdinals.size(); i++) {
                var entry = entriesByNewOrdinal.get(i);
                int originalOrdinal = entry.getKey();
                int newOrdinal = entry.getValue();
                if (!graph.containsNode(originalOrdinal)) {
                    continue;
                }

                out.writeInt(newOrdinal); // unnecessary, but a reasonable sanity check

                for (var feature : orderedFeatures) {
                    feature.writeInline(originalOrdinal, out);
                }

                var neighbors = view.getNeighborsIterator(originalOrdinal);
                out.writeInt(neighbors.size());
                int n = 0;
                for (; n < neighbors.size(); n++) {
                    out.writeInt(oldToNewOrdinals.get(neighbors.nextInt()));
                }
                assert !neighbors.hasNext();

                // pad out to maxEdgesPerNode
                for (; n < graph.maxDegree(); n++) {
                    out.writeInt(-1);
                }
            };
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * @return a Map of old to new graph ordinals where the new ordinals are sequential starting at 0,
     * while preserving the original relative ordering in `graph`.  That is, for all node ids i and j,
     * if i &lt; j in `graph` then map[i] &lt; map[j] in the returned map.
     */
    public static Map<Integer, Integer> getSequentialRenumbering(GraphIndex graph) {
        try (var view = graph.getView()) {
            Int2IntHashMap oldToNewMap = new Int2IntHashMap(-1);
            int nextOrdinal = 0;
            for (int i = 0; i < view.getIdUpperBound(); i++) {
                if (graph.containsNode(i)) {
                    oldToNewMap.put(i, nextOrdinal++);
                }
            }
            return oldToNewMap;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Builder for OnDiskGraphIndexWriter, with optional features.
     */
    public static class Builder {
        private final GraphIndex graphIndex;
        private final Map<Integer, Integer> oldToNewOrdinals;
        private final EnumMap<FeatureId, FeatureWriter> features;

        public Builder(GraphIndex graphIndex) {
            this(graphIndex, getSequentialRenumbering(graphIndex));
        }

        public Builder(GraphIndex graphIndex, Map<Integer, Integer> oldToNewOrdinals) {
            this.graphIndex = graphIndex;
            this.oldToNewOrdinals = oldToNewOrdinals;
            this.features = new EnumMap<>(FeatureId.class);
        }

        public Builder with(FeatureWriter writer) {
            features.put(writer.id(), writer);
            return this;
        }

        public OnDiskGraphIndexWriter build() {
            if (features.containsKey(FeatureId.FUSED_ADC) && !(features.containsKey(FeatureId.LVQ) || features.containsKey(FeatureId.INLINE_VECTORS))) {
                throw new IllegalArgumentException("Fused ADC requires an exact score source.");
            }
            return new OnDiskGraphIndexWriter(graphIndex, oldToNewOrdinals, features);
        }
    }
}
