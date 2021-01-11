package htsjdk.samtools.util.refget;

import mjson.Json;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Class representing a response to a refget service info request as defined in http://samtools.github.io/hts-specs/refget.html
 */
public class RefgetServiceInfoResponse {
    private final boolean circularSupported;
    private final List<String> algorithms;
    private final Integer subsequenceLimit;
    private final List<String> supportedVersions;

    private RefgetServiceInfoResponse(final boolean circularSupported,
                                      final List<String> algorithms,
                                      final Integer subsequenceLimit,
                                      final List<String> supportedVersions) {
        this.circularSupported = circularSupported;
        this.algorithms = algorithms;
        this.subsequenceLimit = subsequenceLimit;
        this.supportedVersions = supportedVersions;
    }

    public static RefgetServiceInfoResponse parse(final Json j) {
        final Json serviceJson = j.at("service");
        if (serviceJson == null || !serviceJson.isObject()) {
            throw new RefgetMalformedResponseException("No service object found");
        }

        final Json circularSupportedJson = serviceJson.at("circular_supported");
        if (circularSupportedJson == null || !circularSupportedJson.isBoolean()) {
            throw new RefgetMalformedResponseException("No circular_supported boolean found inside Refget service info");
        }
        final boolean circularSupported = circularSupportedJson.asBoolean();

        final Json algorithmsJson = serviceJson.at("algorithms");
        if (algorithmsJson == null || !algorithmsJson.isArray()) {
            throw new RefgetMalformedResponseException("No algorithms array found inside Refget service info");
        }
        final List<String> algorithms = algorithmsJson.asJsonList().stream().map(Json::asString).collect(Collectors.toList());

        final Json limitJson = serviceJson.at("subsequence_limit");
        if (limitJson == null || !(limitJson.isNumber() || limitJson.isNull())) {
            throw new RefgetMalformedResponseException("subsequence_limit object inside Refget service info is not of type number or null");
        }
        final Integer limit = limitJson.isNull() ? null : limitJson.asInteger();

        final Json versionsJson = serviceJson.at("supported_api_versions");
        if (versionsJson == null || !versionsJson.isArray()) {
            throw new RefgetMalformedResponseException("No supported_versions array found inside Refget service info");
        }
        final List<String> versions = versionsJson.asJsonList().stream().map(Json::asString).collect(Collectors.toList());

        return new RefgetServiceInfoResponse(circularSupported, algorithms, limit, versions);
    }

    public boolean isCircularSupported() {
        return this.circularSupported;
    }

    public List<String> getAlgorithms() {
        return this.algorithms;
    }

    public Integer getSubsequenceLimit() {
        return this.subsequenceLimit;
    }

    public List<String> getSupportedVersions() {
        return this.supportedVersions;
    }
}
