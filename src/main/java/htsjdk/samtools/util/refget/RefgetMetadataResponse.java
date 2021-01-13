package htsjdk.samtools.util.refget;

import mjson.Json;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class representing a response to a refget metadata request as defined in http://samtools.github.io/hts-specs/refget.html
 */
public class RefgetMetadataResponse {
    private final String md5;
    private final String trunc512;
    private final int length;
    private final List<Alias> aliases;

    private RefgetMetadataResponse(final String md5, final String trunc512, final int length, final List<Alias> aliases) {
        this.md5 = md5;
        this.trunc512 = trunc512;
        this.length = length;
        this.aliases = aliases;
    }

    public static class Alias {
        private final String alias;
        private final String namingAuthority;

        private Alias(final String aliasString, final String authorityString) {
            this.alias = aliasString;
            this.namingAuthority = authorityString;
        }

        private static Alias parse(final Json j) {
            final Json aliasJson = j.at("alias");
            if (aliasJson == null || !aliasJson.isString()) {
                throw new RefgetMalformedResponseException("No alias string found inside Refget metadata alias");
            }
            final String alias = aliasJson.asString();

            final Json authorityJson = j.at("naming_authority");
            if (authorityJson == null || !authorityJson.isString()) {
                throw new RefgetMalformedResponseException("naming_authority object inside Refget metadata alias is not of type string");
            }
            final String authority = authorityJson.asString();

            return new Alias(alias, authority);
        }

        public String getAlias() {
            return this.alias;
        }

        public String getNamingAuthority() {
            return this.namingAuthority;
        }
    }

    public static RefgetMetadataResponse parse(final Json j) {
        final Json dataJson = j.at("metadata");
        if (dataJson == null || !dataJson.isObject()) {
            throw new RefgetMalformedResponseException("No metadata object found");
        }

        final Json md5Json = dataJson.at("md5");
        if (md5Json == null || !md5Json.isString()) {
            throw new RefgetMalformedResponseException("No md5 string found inside metadata");
        }
        final String md5 = md5Json.asString();

        final Json trunc512Json = dataJson.at("trunc512");
        final String trunc512;
        if (trunc512Json == null) {
            throw new RefgetMalformedResponseException("No trunc512 string found inside metadata");
        } else if (trunc512Json.isString()) {
            trunc512 = trunc512Json.asString();
        } else if (trunc512Json.isNull()) {
            trunc512 = null;
        } else {
            throw new RefgetMalformedResponseException("trunc512 object is not of type string or null");
        }

        final Json lengthJson = dataJson.at("length");
        if (lengthJson == null || !lengthJson.isNumber()) {
            throw new RefgetMalformedResponseException("No length number found inside metadata");
        }
        final int length = lengthJson.asInteger();

        final Json aliasesJson = dataJson.at("aliases");
        if (aliasesJson == null || !aliasesJson.isArray()) {
            throw new RefgetMalformedResponseException("No aliases array found inside metadata");
        }
        final List<Alias> aliases = aliasesJson.asJsonList()
            .stream()
            .map(Alias::parse)
            .collect(Collectors.toList());

        return new RefgetMetadataResponse(md5, trunc512, length, aliases);
    }

    public String getMd5() {
        return this.md5;
    }

    public String getTrunc512() {
        return this.trunc512;
    }

    public int getLength() {
        return this.length;
    }

    public List<Alias> getAliases() {
        return Collections.unmodifiableList(this.aliases);
    }
}