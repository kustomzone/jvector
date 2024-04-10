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

package io.github.jbellis.jvector.example;

import io.github.jbellis.jvector.example.util.CompressorParameters;
import io.github.jbellis.jvector.example.util.DataSet;
import io.github.jbellis.jvector.example.util.ReaderSupplierFactory;
import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.CachingGraphIndex;
import io.github.jbellis.jvector.graph.disk.FusedADC;
import io.github.jbellis.jvector.graph.disk.InlineVectors;
import io.github.jbellis.jvector.graph.disk.LVQ;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.pq.LocallyAdaptiveVectorQuantization;
import io.github.jbellis.jvector.pq.PQVectors;
import io.github.jbellis.jvector.pq.ProductQuantization;
import io.github.jbellis.jvector.pq.VectorCompressor;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.ExplicitThreadLocal;
import io.github.jbellis.jvector.util.PhysicalCoreExecutor;
import io.github.jbellis.jvector.vector.types.VectorFloat;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Tests a grid of configurations against a dataset
 */
public class Grid {
    private static final String pqCacheDir = "pq_cache";

    private static final String dirPrefix = "BenchGraphDir";

    static void runAll(DataSet ds,
                       List<Integer> mGrid,
                       List<Integer> efConstructionGrid,
                       List<Function<DataSet, CompressorParameters>> buildCompressors,
                       List<Function<DataSet, CompressorParameters>> compressionGrid,
                       List<Integer> efSearchFactor) throws IOException
    {
        var testDirectory = Files.createTempDirectory(dirPrefix);
        try {
            for (int M : mGrid) {
                for (int efC : efConstructionGrid) {
                    for (var bc : buildCompressors) {
                        var compressor = getCompressor(bc, ds);
                        runOneGraph(M, efC, compressor, compressionGrid, efSearchFactor, ds, testDirectory);
                    }
                }
            }
        } finally {
            Files.delete(testDirectory);
            cachedCompressors.clear();
        }
    }

    static void runOneGraph(int M,
                            int efConstruction,
                            VectorCompressor<?> buildCompressor,
                            List<Function<DataSet, CompressorParameters>> compressionGrid,
                            List<Integer> efSearchOptions,
                            DataSet ds,
                            Path testDirectory) throws IOException
    {
        var floatVectors = ds.getBaseRavv();
        var dimension = ds.getDimension();
        GraphIndexBuilder builder;
        if (buildCompressor == null) {
            var bsp = BuildScoreProvider.randomAccessScoreProvider(floatVectors, ds.similarityFunction);
            builder = new GraphIndexBuilder(bsp, floatVectors.dimension(), M, efConstruction, 1.2f, 1.2f,
                                            PhysicalCoreExecutor.pool(), ForkJoinPool.commonPool());
        } else {
            var quantized = buildCompressor.encodeAll(ds.baseVectors);
            var pq = (PQVectors) buildCompressor.createCompressedVectors(quantized);
            var ravv = new ListRandomAccessVectorValues(ds.baseVectors, ds.baseVectors.get(0).length());
            var bsp = BuildScoreProvider.pqBuildScoreProvider(ds.similarityFunction, ravv, pq);
            builder = new GraphIndexBuilder(bsp, floatVectors.dimension(), M, efConstruction, 1.5f, 1.2f,
                                            PhysicalCoreExecutor.pool(), ForkJoinPool.commonPool());
        }
        var start = System.nanoTime();
        var onHeapGraph = builder.build(floatVectors);
        System.out.format("Build (%s) M=%d ef=%d in %.2fs with avg degree %.2f and %.2f short edges%n",
                          buildCompressor == null ? "full res" : buildCompressor.toString(),
                          M,
                          efConstruction,
                          (System.nanoTime() - start) / 1_000_000_000.0,
                          onHeapGraph.getAverageDegree(),
                          onHeapGraph.getAverageShortEdges());

        var graphPath = testDirectory.resolve("graph" + M + efConstruction + ds.name);
        var fusedGraphPath = testDirectory.resolve("fusedgraph" + M + efConstruction + ds.name);
        var lvqGraphPath = testDirectory.resolve("lvqgraph" + M + efConstruction + ds.name);
        try {
            try (var outputStream = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(graphPath)))) {
                var writer = new OnDiskGraphIndexWriter.Builder(onHeapGraph)
                        .with(new InlineVectors(floatVectors.dimension()).asWriter(floatVectors)).build();
                writer.write(outputStream);
            }

            var lvq = LocallyAdaptiveVectorQuantization.compute(floatVectors);
            try (var outputStream = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(lvqGraphPath)))) {
                var writer = new OnDiskGraphIndexWriter.Builder(onHeapGraph)
                        .with(new LVQ(lvq, dimension% 64 == 0 ? dimension : (dimension / 64 + 1) * 64 + 2 * Float.BYTES).asWriter(lvq.encodeAll(ds.baseVectors))).build();
                writer.write(outputStream);
            }

            for (var cpSupplier : compressionGrid) {
                var compressor = getCompressor(cpSupplier, ds);
                CompressedVectors cv = null;
                var fusedCompatible = compressor instanceof ProductQuantization && ((ProductQuantization) compressor).getClusterCount() == 32;
                if (compressor == null) {
                    System.out.format("Uncompressed vectors%n");
                } else {
                    start = System.nanoTime();
                    var quantizedVectors = compressor.encodeAll(ds.baseVectors);
                    cv = compressor.createCompressedVectors(quantizedVectors);
                    System.out.format("%s encoded %d vectors [%.2f MB] in %.2fs%n", compressor, ds.baseVectors.size(), (cv.ramBytesUsed() / 1024f / 1024f), (System.nanoTime() - start) / 1_000_000_000.0);

                    if (fusedCompatible) {
                        try (var outputStream = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(fusedGraphPath)));
                             var view = onHeapGraph.getView()) {
                            var writer = new OnDiskGraphIndexWriter.Builder(onHeapGraph)
                                    .with(new LVQ(lvq, dimension% 64 == 0 ? dimension : (dimension / 64 + 1) * 64 + 2 * Float.BYTES).asWriter(lvq.encodeAll(ds.baseVectors)))
                                    .with(new FusedADC(onHeapGraph.maxDegree(), ((PQVectors) cv).getProductQuantization()).asWriter(view, (PQVectors) cv))
                                    .build();
                            writer.write(outputStream);
                        }
                    }
                }

                try (var onDiskGraph = new CachingGraphIndex((OnDiskGraphIndex.load(ReaderSupplierFactory.open(graphPath), 0)));
                     var onDiskLVQGraph = compressor == null ? null : new CachingGraphIndex(OnDiskGraphIndex.load(ReaderSupplierFactory.open(lvqGraphPath), 0));
                     var onDiskFusedGraph = fusedCompatible ? new CachingGraphIndex(OnDiskGraphIndex.load(ReaderSupplierFactory.open(fusedGraphPath), 0)) : null)
                {
                    List<GraphIndex> graphs = new ArrayList<>();
                    graphs.add(onDiskGraph);
                    if (onDiskFusedGraph != null) {
                        graphs.add(onDiskFusedGraph);
                    }
                    if (onDiskLVQGraph != null) {
                        graphs.add(onDiskLVQGraph);
                    }
                    if (cv == null) {
                        graphs.add(onHeapGraph); // if we have no cv, compare on-heap/on-disk with exact searches
                    }
                    for (var g : graphs) {
                        try (var cs = new ConfiguredSystem(ds, g, cv)) {
                            testConfiguration(cs, efSearchOptions);
                        }
                    }
                }
            }
        } finally {
            Files.deleteIfExists(graphPath);
            Files.deleteIfExists(fusedGraphPath);
            Files.deleteIfExists(lvqGraphPath);
        }
    }

    // avoid recomputing the compressor repeatedly (this is a relatively small memory footprint)
    static final Map<String, VectorCompressor<?>> cachedCompressors = new IdentityHashMap<>();

    private static void testConfiguration(ConfiguredSystem cs, List<Integer> efSearchOptions) {
        var topK = cs.ds.groundTruth.get(0).size();
        System.out.format("Using %s:%n", cs.index);
        for (int overquery : efSearchOptions) {
            var start = System.nanoTime();
            var pqr = performQueries(cs, topK, topK * overquery, 2);
            var recall = ((double) pqr.topKFound) / (2 * cs.ds.queryVectors.size() * topK);
            System.out.format(" Query top %d/%d recall %.4f in %.2fs after %,d nodes visited%n",
                              topK, overquery, recall, (System.nanoTime() - start) / 1_000_000_000.0, pqr.nodesVisited);

        }
    }

    private static VectorCompressor<?> getCompressor(Function<DataSet, CompressorParameters> cpSupplier, DataSet ds) {
        var cp = cpSupplier.apply(ds);
        if (!cp.supportsCaching()) {
            return cp.computeCompressor(ds);
        }

        var fname = cp.idStringFor(ds);
        return cachedCompressors.computeIfAbsent(fname, __ -> {
            var path = Paths.get(pqCacheDir).resolve(fname);
            if (path.toFile().exists()) {
                try {
                    try (var readerSupplier = ReaderSupplierFactory.open(path)) {
                        try (var rar = readerSupplier.get()) {
                            var pq = ProductQuantization.load(rar);
                            System.out.format("%s loaded from %s%n", pq, fname);
                            return pq;
                        }
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            var start = System.nanoTime();
            var compressor = cp.computeCompressor(ds);
            System.out.format("%s build in %.2fs,%n", compressor, (System.nanoTime() - start) / 1_000_000_000.0);
            if (cp.supportsCaching()) {
                try {
                    Files.createDirectories(path.getParent());
                    try (var writer = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)))) {
                        compressor.write(writer);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            return compressor;
        });
    }

    private static long topKCorrect(int topK, int[] resultNodes, Set<Integer> gt) {
        int count = Math.min(resultNodes.length, topK);
        var resultSet = Arrays.stream(resultNodes, 0, count)
                .boxed()
                .collect(Collectors.toSet());
        assert resultSet.size() == count : String.format("%s duplicate results out of %s", count - resultSet.size(), count);
        return resultSet.stream().filter(gt::contains).count();
    }

    private static long topKCorrect(int topK, SearchResult.NodeScore[] nn, Set<Integer> gt) {
        var a = Arrays.stream(nn).mapToInt(nodeScore -> nodeScore.node).toArray();
        return topKCorrect(topK, a, gt);
    }

    private static ResultSummary performQueries(ConfiguredSystem cs, int topK, int efSearch, int queryRuns) {
        assert efSearch >= topK;
        LongAdder topKfound = new LongAdder();
        LongAdder nodesVisited = new LongAdder();
        for (int k = 0; k < queryRuns; k++) {
            IntStream.range(0, cs.ds.queryVectors.size()).parallel().forEach(i -> {
                var queryVector = cs.ds.queryVectors.get(i);
                SearchResult sr;
                var searcher = cs.getSearcher();
                var sf = cs.scoreProviderFor(queryVector, searcher.getView());
                sr = searcher.search(sf, efSearch, Bits.ALL);

                // process search result
                var gt = cs.ds.groundTruth.get(i);
                var n = topKCorrect(topK, sr.getNodes(), gt);
                topKfound.add(n);
                nodesVisited.add(sr.getVisitedCount());
            });
        }
        return new ResultSummary((int) topKfound.sum(), nodesVisited.sum());
    }

    static class ConfiguredSystem implements AutoCloseable {
        DataSet ds;
        GraphIndex index;
        CompressedVectors cv;

        private final ExplicitThreadLocal<GraphSearcher> searchers = ExplicitThreadLocal.withInitial(() -> {
            return new GraphSearcher(index.getView());
        });

        ConfiguredSystem(DataSet ds, GraphIndex index, CompressedVectors cv) {
            this.ds = ds;
            this.index = index;
            this.cv = cv;
        }

        public SearchScoreProvider scoreProviderFor(VectorFloat<?> queryVector, GraphIndex.View view) {
            // if we're not compressing then just use the exact score function
            if (cv == null) {
                var sf = ScoreFunction.ExactScoreFunction.from(queryVector, ds.similarityFunction, ds.getBaseRavv());
                return new SearchScoreProvider(sf, null);
            }

            var scoringView = (GraphIndex.ScoringView) view;
            ScoreFunction.ApproximateScoreFunction asf;
            try {
                asf = scoringView.approximateScoreFunctionFor(queryVector, ds.similarityFunction);
            } catch (UnsupportedOperationException e) {
                asf = cv.precomputedScoreFunctionFor(queryVector, ds.similarityFunction);
            }
            ScoreFunction.ExactScoreFunction rr = scoringView.rerankerFor(queryVector, ds.similarityFunction);
            return new SearchScoreProvider(asf, rr);
        }

        public GraphSearcher getSearcher() {
            return searchers.get();
        }

        @Override
        public void close() {
            searchers.close();
        }
    }

    static class ResultSummary {
        final int topKFound;
        final long nodesVisited;

        ResultSummary(int topKFound, long nodesVisited) {
            this.topKFound = topKFound;
            this.nodesVisited = nodesVisited;
        }
    }
}
