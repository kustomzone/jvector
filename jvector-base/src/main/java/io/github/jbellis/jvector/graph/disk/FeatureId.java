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

import io.github.jbellis.jvector.disk.RandomAccessReader;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * An enum representing the features that can be stored in an on-disk graph index.
 * These are typically mapped to a Feature or FeatureWriter.
 */
public enum FeatureId {
    INLINE_VECTORS(0, InlineVectors::load),
    FUSED_ADC(1, FusedADC::load),
    LVQ(2, io.github.jbellis.jvector.graph.disk.LVQ::load);

    public static final List<FeatureId> ALL_SORTED = Arrays.stream(FeatureId.values()).sorted(Comparator.comparingInt(a -> a.bitshift))
            .collect(Collectors.toUnmodifiableList());
    public static final Set<FeatureId> ALL = Collections.unmodifiableSet(EnumSet.allOf(FeatureId.class));

    private final BiFunction<CommonHeader, RandomAccessReader, Feature> loader;
    private final int bitshift;

    FeatureId(int bitshift, BiFunction<CommonHeader, RandomAccessReader, Feature> loader) {
        this.bitshift = bitshift;
        this.loader = loader;
    }

    Feature load(CommonHeader header, RandomAccessReader reader) {
        return loader.apply(header, reader);
    }

    public static EnumSet<FeatureId> deserialize(int bitflags) {
        EnumSet<FeatureId> set = EnumSet.noneOf(FeatureId.class);
        for (int n = 0; n < values().length; n++) {
            if ((bitflags & (1 << n)) != 0)
                set.add(ALL_SORTED.get(n));
        }
        return set;
    }

    public static int serialize(EnumSet<FeatureId> flags) {
        int i = 0;
        for (FeatureId flag : flags)
            i |= 1 << flag.bitshift;
        return i;
    }

    public int bitshift() {
        return bitshift;
    }
}
