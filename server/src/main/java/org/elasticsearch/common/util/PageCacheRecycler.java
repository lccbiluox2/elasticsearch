/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.recycler.AbstractRecyclerC;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;

import java.util.Arrays;
import java.util.Locale;

import static org.elasticsearch.common.recycler.Recyclers.concurrent;
import static org.elasticsearch.common.recycler.Recyclers.concurrentDeque;
import static org.elasticsearch.common.recycler.Recyclers.dequeFactory;
import static org.elasticsearch.common.recycler.Recyclers.none;

/** A recycler of fixed-size pages.
 * 固定大小页面的回收者。
 * */
public class PageCacheRecycler {

    public static final Setting<Type> TYPE_SETTING =
        new Setting<>("cache.recycler.page.type", Type.CONCURRENT.name(), Type::parse, Property.NodeScope);
    public static final Setting<ByteSizeValue> LIMIT_HEAP_SETTING  =
        Setting.memorySizeSetting("cache.recycler.page.limit.heap", "10%", Property.NodeScope);
    public static final Setting<Double> WEIGHT_BYTES_SETTING  =
        Setting.doubleSetting("cache.recycler.page.weight.bytes", 1d, 0d, Property.NodeScope);
    public static final Setting<Double> WEIGHT_LONG_SETTING  =
        Setting.doubleSetting("cache.recycler.page.weight.longs", 1d, 0d, Property.NodeScope);
    public static final Setting<Double> WEIGHT_INT_SETTING  =
        Setting.doubleSetting("cache.recycler.page.weight.ints", 1d, 0d, Property.NodeScope);
    // object pages are less useful to us so we give them a lower weight by default
    public static final Setting<Double> WEIGHT_OBJECTS_SETTING  =
        Setting.doubleSetting("cache.recycler.page.weight.objects", 0.1d, 0d, Property.NodeScope);

    /** Page size in bytes: 16KB */
    public static final int PAGE_SIZE_IN_BYTES = 1 << 14;
    public static final int OBJECT_PAGE_SIZE = PAGE_SIZE_IN_BYTES / RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    public static final int LONG_PAGE_SIZE = PAGE_SIZE_IN_BYTES / Long.BYTES;
    public static final int INT_PAGE_SIZE = PAGE_SIZE_IN_BYTES / Integer.BYTES;
    public static final int BYTE_PAGE_SIZE = PAGE_SIZE_IN_BYTES;

    private final Recycler<byte[]> bytePage;
    private final Recycler<int[]> intPage;
    private final Recycler<long[]> longPage;
    private final Recycler<Object[]> objectPage;

    public static final PageCacheRecycler NON_RECYCLING_INSTANCE;

    static {
        NON_RECYCLING_INSTANCE = new PageCacheRecycler(Settings.builder().put(LIMIT_HEAP_SETTING.getKey(), "0%").build());
    }

    public PageCacheRecycler(Settings settings) {
        // 获取配置中的 page 回收类型
        final Type type = TYPE_SETTING.get(settings);
        // cache.recycler.page.limit.heap 获取限制大小
        final long limit = LIMIT_HEAP_SETTING.get(settings).getBytes();
        // 获取分配的处理器个数
        final int allocatedProcessors = EsExecutors.allocatedProcessors(settings);

        // We have a global amount of memory that we need to divide across data types.
        // Since some types are more useful than other ones we give them different weights.
        // Trying to store all of them in a single stack would be problematic because eg.
        // a work load could fill the recycler with only byte[] pages and then another
        // workload that would work with double[] pages couldn't recycle them because there
        // is no space left in the stack/queue. LRU/LFU policies are not an option either
        // because they would make obtain/release too costly: we really need constant-time
        // operations.
        // Ultimately a better solution would be to only store one kind of data and have the
        // ability to interpret it either as a source of bytes, doubles, longs, etc. eg. thanks
        // to direct ByteBuffers or sun.misc.Unsafe on a byte[] but this would have other issues
        // that would need to be addressed such as garbage collection of native memory or safety
        // of Unsafe writes.

        // 我们有一个全局的内存量，我们需要将其划分到不同的数据类型。由于某些类型比其他类型更有用，我们给它们不同的权重。
        // 尝试将它们全部存储在一个堆栈中会有问题，因为。一个工作负载可能只使用 byte[] 页填充回收器，然后另一个可以使用
        // double[] 页的工作负载无法回收它们，因为堆栈/队列中没有剩余空间。LRU/LFU策略也不是一个选项，因为它们会让
        // 获取/发布的代价太大:我们真的需要常量时间
        final double bytesWeight = WEIGHT_BYTES_SETTING .get(settings);  // 字节权重
        final double intsWeight = WEIGHT_INT_SETTING .get(settings);    // int权重
        final double longsWeight = WEIGHT_LONG_SETTING .get(settings);// long权重
        final double objectsWeight = WEIGHT_OBJECTS_SETTING .get(settings);// 对象权重

        // 总的权重
        final double totalWeight = bytesWeight + intsWeight + longsWeight + objectsWeight;
        // 最大页大小
        final int maxPageCount = (int) Math.min(Integer.MAX_VALUE, limit / PAGE_SIZE_IN_BYTES);

        // 最大字节页大小
        final int maxBytePageCount = (int) (bytesWeight * maxPageCount / totalWeight);
        bytePage = build(type, maxBytePageCount, allocatedProcessors, new AbstractRecyclerC<byte[]>() {
            @Override
            public byte[] newInstance() {
                return new byte[BYTE_PAGE_SIZE];
            }
            @Override
            public void recycle(byte[] value) {
                // nothing to do
            }
        });

        final int maxIntPageCount = (int) (intsWeight * maxPageCount / totalWeight);
        intPage = build(type, maxIntPageCount, allocatedProcessors, new AbstractRecyclerC<int[]>() {
            @Override
            public int[] newInstance() {
                return new int[INT_PAGE_SIZE];
            }
            @Override
            public void recycle(int[] value) {
                // nothing to do
            }
        });

        final int maxLongPageCount = (int) (longsWeight * maxPageCount / totalWeight);
        longPage = build(type, maxLongPageCount, allocatedProcessors, new AbstractRecyclerC<long[]>() {
            @Override
            public long[] newInstance() {
                return new long[LONG_PAGE_SIZE];
            }
            @Override
            public void recycle(long[] value) {
                // nothing to do
            }
        });

        final int maxObjectPageCount = (int) (objectsWeight * maxPageCount / totalWeight);
        objectPage = build(type, maxObjectPageCount, allocatedProcessors, new AbstractRecyclerC<Object[]>() {
            @Override
            public Object[] newInstance() {
                return new Object[OBJECT_PAGE_SIZE];
            }
            @Override
            public void recycle(Object[] value) {
                Arrays.fill(value, null); // we need to remove the strong refs on the objects stored in the array
            }
        });

        assert PAGE_SIZE_IN_BYTES * (maxBytePageCount + maxIntPageCount + maxLongPageCount + maxObjectPageCount) <= limit;
    }

    public Recycler.V<byte[]> bytePage(boolean clear) {
        final Recycler.V<byte[]> v = bytePage.obtain();
        if (v.isRecycled() && clear) {
            Arrays.fill(v.v(), (byte) 0);
        }
        return v;
    }

    public Recycler.V<int[]> intPage(boolean clear) {
        final Recycler.V<int[]> v = intPage.obtain();
        if (v.isRecycled() && clear) {
            Arrays.fill(v.v(), 0);
        }
        return v;
    }

    public Recycler.V<long[]> longPage(boolean clear) {
        final Recycler.V<long[]> v = longPage.obtain();
        if (v.isRecycled() && clear) {
            Arrays.fill(v.v(), 0L);
        }
        return v;
    }

    public Recycler.V<Object[]> objectPage() {
        // object pages are cleared on release anyway
        return objectPage.obtain();
    }

    private static <T> Recycler<T> build(Type type, int limit, int availableProcessors, Recycler.C<T> c) {
        final Recycler<T> recycler;
        if (limit == 0) {
            recycler = none(c);
        } else {
            recycler = type.build(c, limit, availableProcessors);
        }
        return recycler;
    }

    public enum Type {
        QUEUE {
            @Override
            <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors) {
                return concurrentDeque(c, limit);
            }
        },
        CONCURRENT {
            @Override
            <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors) {
                return concurrent(dequeFactory(c, limit / availableProcessors), availableProcessors);
            }
        },
        NONE {
            @Override
            <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors) {
                return none(c);
            }
        };

        public static Type parse(String type) {
            try {
                return Type.valueOf(type.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("no type support [" + type + "]");
            }
        }

        abstract <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors);
    }
}
