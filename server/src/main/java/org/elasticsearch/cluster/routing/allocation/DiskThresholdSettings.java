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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * A container to keep settings for disk thresholds up to date with cluster setting changes.
 */
public class DiskThresholdSettings {
    /**
     * 是否开启基于磁盘的分片分配,默认true
     */
    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING =
        Setting.boolSetting("cluster.routing.allocation.disk.threshold_enabled", true,
            Setting.Property.Dynamic, Setting.Property.NodeScope);
    /**
     * 低警戒水位线——默认为磁盘容量的85％。
     * Elasticsearch不会将分片分配给使用磁盘超过85％的节点。它也可以设置为绝对字节值（如500mb），
     * 以防止Elasticsearch在小于指定的可用空间量时分配分片。此设置不会影响新创建的索引的主分片，
     * 或者特别是之前任何从未分配过的分片。
     */
    public static final Setting<String> CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING =
        new Setting<>("cluster.routing.allocation.disk.watermark.low", "85%",
            (s) -> validWatermarkSetting(s, "cluster.routing.allocation.disk.watermark.low"),
            new LowDiskWatermarkValidator(),
            Setting.Property.Dynamic, Setting.Property.NodeScope);
    /**
     * 高警戒水位线——默认为磁盘容量的90％。
     * Elasticsearch将尝试从磁盘使用率超过90％的节点重新分配分片。它也可以设置为绝对字节值，以便在节点
     * 小于指定的可用空间量时将其从节点重新分配。此设置会影响所有分片的分配，无论先前是否分配。
     */
    public static final Setting<String> CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING =
        new Setting<>("cluster.routing.allocation.disk.watermark.high", "90%",
            (s) -> validWatermarkSetting(s, "cluster.routing.allocation.disk.watermark.high"),
            new HighDiskWatermarkValidator(),
            Setting.Property.Dynamic, Setting.Property.NodeScope);
    /**
     * 洪水警戒水位线——默认为磁盘容量的95％。
     * Elasticsearch对每个索引强制执行只读索引块（index.blocks.read_only_allow_delete）。
     * 这是防止节点耗尽磁盘空间的最后手段。一旦有足够的可用磁盘空间允许索引操作继续，就必须手动释放索引块。
     */
    public static final Setting<String> CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING =
        new Setting<>("cluster.routing.allocation.disk.watermark.flood_stage", "95%",
            (s) -> validWatermarkSetting(s, "cluster.routing.allocation.disk.watermark.flood_stage"),
            new FloodStageValidator(),
            Setting.Property.Dynamic, Setting.Property.NodeScope);
    /**
     * 当计算一个节点的剩余磁盘空间时，是否考虑正在重新分配到当前节点的分片容量，默认true。
     * 这可能导致错误的高估一个磁盘的使用率。因为分片重分配可能已经完成了90%，检索到的磁盘使用率包含了这个
     * 重新分配的分片总大小以及这已经分配了的90%进度的大小。
     *
     * 链接：https://www.jianshu.com/p/55fd8a0b120b
     */
    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING =
        Setting.boolSetting("cluster.routing.allocation.disk.include_relocations", true,
            Setting.Property.Dynamic, Setting.Property.NodeScope, Setting.Property.Deprecated);
    /**
     * 分片重分配间隔，默认60秒
     */
    public static final Setting<TimeValue> CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING =
        Setting.positiveTimeSetting("cluster.routing.allocation.disk.reroute_interval", TimeValue.timeValueSeconds(60),
            Setting.Property.Dynamic, Setting.Property.NodeScope);

    private volatile String lowWatermarkRaw;
    private volatile String highWatermarkRaw;
    private volatile Double freeDiskThresholdLow;
    private volatile Double freeDiskThresholdHigh;
    private volatile ByteSizeValue freeBytesThresholdLow;
    private volatile ByteSizeValue freeBytesThresholdHigh;
    private volatile boolean includeRelocations;
    private volatile boolean enabled;
    private volatile TimeValue rerouteInterval;
    private volatile Double freeDiskThresholdFloodStage;
    private volatile ByteSizeValue freeBytesThresholdFloodStage;
    private static final boolean autoReleaseIndexEnabled;
    public static final String AUTO_RELEASE_INDEX_ENABLED_KEY = "es.disk.auto_release_flood_stage_block";

    static {
        final String property = System.getProperty(AUTO_RELEASE_INDEX_ENABLED_KEY);
        if (property == null) {
            autoReleaseIndexEnabled = true;
        } else if (Boolean.FALSE.toString().equals(property)){
            autoReleaseIndexEnabled = false;
        } else {
            throw new IllegalArgumentException(AUTO_RELEASE_INDEX_ENABLED_KEY + " may only be unset or set to [false] but was [" +
                property + "]");
        }
    }

    public DiskThresholdSettings(Settings settings, ClusterSettings clusterSettings) {
        final String lowWatermark = CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.get(settings);
        final String highWatermark = CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.get(settings);
        final String floodStage = CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.get(settings);
        setHighWatermark(highWatermark);
        setLowWatermark(lowWatermark);
        setFloodStage(floodStage);
        this.includeRelocations = CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING.get(settings);
        this.rerouteInterval = CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(settings);
        this.enabled = CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING, this::setLowWatermark);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING, this::setHighWatermark);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING, this::setFloodStage);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING, this::setIncludeRelocations);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING, this::setRerouteInterval);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING, this::setEnabled);
    }

    static final class LowDiskWatermarkValidator implements Setting.Validator<String> {

        @Override
        public void validate(String value) {

        }

        @Override
        public void validate(final String value, final Map<Setting<?>, Object> settings) {
            final String highWatermarkRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING);
            final String floodStageRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING);
            doValidate(value, highWatermarkRaw, floodStageRaw);
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = Arrays.asList(
                    CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING,
                    CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING);
            return settings.iterator();
        }

    }

    static final class HighDiskWatermarkValidator implements Setting.Validator<String> {

        @Override
        public void validate(final String value) {

        }

        @Override
        public void validate(final String value, final Map<Setting<?>, Object> settings) {
            final String lowWatermarkRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING);
            final String floodStageRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING);
            doValidate(lowWatermarkRaw, value, floodStageRaw);
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = Arrays.asList(
                    CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING,
                    CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING);
            return settings.iterator();
        }

    }

    static final class FloodStageValidator implements Setting.Validator<String> {

        @Override
        public void validate(final String value) {

        }

        @Override
        public void validate(final String value, final Map<Setting<?>, Object> settings) {
            final String lowWatermarkRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING);
            final String highWatermarkRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING);
            doValidate(lowWatermarkRaw, highWatermarkRaw, value);
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = Arrays.asList(
                    CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING,
                    CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING);
            return settings.iterator();
        }

    }

    private static void doValidate(String low, String high, String flood) {
        try {
            doValidateAsPercentage(low, high, flood);
            return; // early return so that we do not try to parse as bytes
        } catch (final ElasticsearchParseException e) {
            // swallow as we are now going to try to parse as bytes
        }
        try {
            doValidateAsBytes(low, high, flood);
        } catch (final ElasticsearchParseException e) {
            final String message = String.format(
                    Locale.ROOT,
                    "unable to consistently parse [%s=%s], [%s=%s], and [%s=%s] as percentage or bytes",
                    CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
                    low,
                    CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                    high,
                    CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
                    flood);
            throw new IllegalArgumentException(message, e);
        }
    }

    private static void doValidateAsPercentage(final String low, final String high, final String flood) {
        final double lowWatermarkThreshold = thresholdPercentageFromWatermark(low, false);
        final double highWatermarkThreshold = thresholdPercentageFromWatermark(high, false);
        final double floodThreshold = thresholdPercentageFromWatermark(flood, false);
        if (lowWatermarkThreshold > highWatermarkThreshold) {
            throw new IllegalArgumentException(
                    "low disk watermark [" + low + "] more than high disk watermark [" + high + "]");
        }
        if (highWatermarkThreshold > floodThreshold) {
            throw new IllegalArgumentException(
                    "high disk watermark [" + high + "] more than flood stage disk watermark [" + flood + "]");
        }
    }

    private static void doValidateAsBytes(final String low, final String high, final String flood) {
        final ByteSizeValue lowWatermarkBytes =
                thresholdBytesFromWatermark(low, CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), false);
        final ByteSizeValue highWatermarkBytes =
                thresholdBytesFromWatermark(high, CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), false);
        final ByteSizeValue floodStageBytes =
                thresholdBytesFromWatermark(flood, CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), false);
        if (lowWatermarkBytes.getBytes() < highWatermarkBytes.getBytes()) {
            throw new IllegalArgumentException(
                    "low disk watermark [" + low + "] less than high disk watermark [" + high + "]");
        }
        if (highWatermarkBytes.getBytes() < floodStageBytes.getBytes()) {
            throw new IllegalArgumentException(
                    "high disk watermark [" + high + "] less than flood stage disk watermark [" + flood + "]");
        }
    }

    private void setIncludeRelocations(boolean includeRelocations) {
        this.includeRelocations = includeRelocations;
    }

    private void setRerouteInterval(TimeValue rerouteInterval) {
        this.rerouteInterval = rerouteInterval;
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    private void setLowWatermark(String lowWatermark) {
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.lowWatermarkRaw = lowWatermark;
        this.freeDiskThresholdLow = 100.0 - thresholdPercentageFromWatermark(lowWatermark);
        this.freeBytesThresholdLow = thresholdBytesFromWatermark(lowWatermark,
            CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey());
    }

    private void setHighWatermark(String highWatermark) {
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.highWatermarkRaw = highWatermark;
        this.freeDiskThresholdHigh = 100.0 - thresholdPercentageFromWatermark(highWatermark);
        this.freeBytesThresholdHigh = thresholdBytesFromWatermark(highWatermark,
            CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey());
    }

    private void setFloodStage(String floodStageRaw) {
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.freeDiskThresholdFloodStage = 100.0 - thresholdPercentageFromWatermark(floodStageRaw);
        this.freeBytesThresholdFloodStage = thresholdBytesFromWatermark(floodStageRaw,
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey());
    }

    /**
     * Gets the raw (uninterpreted) low watermark value as found in the settings.
     */
    public String getLowWatermarkRaw() {
        return lowWatermarkRaw;
    }

    /**
     * Gets the raw (uninterpreted) high watermark value as found in the settings.
     */
    public String getHighWatermarkRaw() {
        return highWatermarkRaw;
    }

    public Double getFreeDiskThresholdLow() {
        return freeDiskThresholdLow;
    }

    public Double getFreeDiskThresholdHigh() {
        return freeDiskThresholdHigh;
    }

    public ByteSizeValue getFreeBytesThresholdLow() {
        return freeBytesThresholdLow;
    }

    public ByteSizeValue getFreeBytesThresholdHigh() {
        return freeBytesThresholdHigh;
    }

    public Double getFreeDiskThresholdFloodStage() {
        return freeDiskThresholdFloodStage;
    }

    public ByteSizeValue getFreeBytesThresholdFloodStage() {
        return freeBytesThresholdFloodStage;
    }

    public boolean isAutoReleaseIndexEnabled() {
        return autoReleaseIndexEnabled;
    }

    public boolean includeRelocations() {
        return includeRelocations;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public TimeValue getRerouteInterval() {
        return rerouteInterval;
    }

    String describeLowThreshold() {
        return freeBytesThresholdLow.equals(ByteSizeValue.ZERO)
            ? Strings.format1Decimals(100.0 - freeDiskThresholdLow, "%")
            : freeBytesThresholdLow.toString();
    }

    String describeHighThreshold() {
        return freeBytesThresholdHigh.equals(ByteSizeValue.ZERO)
            ? Strings.format1Decimals(100.0 - freeDiskThresholdHigh, "%")
            : freeBytesThresholdHigh.toString();
    }

    String describeFloodStageThreshold() {
        return freeBytesThresholdFloodStage.equals(ByteSizeValue.ZERO)
            ? Strings.format1Decimals(100.0 - freeDiskThresholdFloodStage, "%")
            : freeBytesThresholdFloodStage.toString();
    }

    /**
     * Attempts to parse the watermark into a percentage, returning 100.0% if
     * it cannot be parsed.
     */
    private static double thresholdPercentageFromWatermark(String watermark) {
        return thresholdPercentageFromWatermark(watermark, true);
    }

    /**
     * Attempts to parse the watermark into a percentage, returning 100.0% if it can not be parsed and the specified lenient parameter is
     * true, otherwise throwing an {@link ElasticsearchParseException}.
     *
     * @param watermark the watermark to parse as a percentage
     * @param lenient true if lenient parsing should be applied
     * @return the parsed percentage
     */
    private static double thresholdPercentageFromWatermark(String watermark, boolean lenient) {
        try {
            return RatioValue.parseRatioValue(watermark).getAsPercent();
        } catch (ElasticsearchParseException ex) {
            // NOTE: this is not end-user leniency, since up above we check that it's a valid byte or percentage, and then store the two
            // cases separately
            if (lenient) {
                return 100.0;
            }
            throw ex;
        }
    }

    /**
     * Attempts to parse the watermark into a {@link ByteSizeValue}, returning
     * a ByteSizeValue of 0 bytes if the value cannot be parsed.
     */
    private static ByteSizeValue thresholdBytesFromWatermark(String watermark, String settingName) {
        return thresholdBytesFromWatermark(watermark, settingName, true);
    }

    /**
     * Attempts to parse the watermark into a {@link ByteSizeValue}, returning zero bytes if it can not be parsed and the specified lenient
     * parameter is true, otherwise throwing an {@link ElasticsearchParseException}.
     *
     * @param watermark the watermark to parse as a byte size
     * @param settingName the name of the setting
     * @param lenient true if lenient parsing should be applied
     * @return the parsed byte size value
     */
    private static ByteSizeValue thresholdBytesFromWatermark(String watermark, String settingName, boolean lenient) {
        try {
            return ByteSizeValue.parseBytesSizeValue(watermark, settingName);
        } catch (ElasticsearchParseException ex) {
            // NOTE: this is not end-user leniency, since up above we check that it's a valid byte or percentage, and then store the two
            // cases separately
            if (lenient) {
                return ByteSizeValue.parseBytesSizeValue("0b", settingName);
            }
            throw ex;
        }
    }

    /**
     * Checks if a watermark string is a valid percentage or byte size value,
     * @return the watermark value given
     */
    private static String validWatermarkSetting(String watermark, String settingName) {
        try {
            RatioValue.parseRatioValue(watermark);
        } catch (ElasticsearchParseException e) {
            try {
                ByteSizeValue.parseBytesSizeValue(watermark, settingName);
            } catch (ElasticsearchParseException ex) {
                ex.addSuppressed(e);
                throw ex;
            }
        }
        return watermark;
    }
}
