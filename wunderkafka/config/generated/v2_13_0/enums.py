######################################################################
########### THIS FILE IS GENERATED, DO NOT EDIT MANUALLY!!! ##########
########### THIS FILE IS GENERATED, DO NOT EDIT MANUALLY!!! ##########
########### THIS FILE IS GENERATED, DO NOT EDIT MANUALLY!!! ##########
########### THIS FILE IS GENERATED, DO NOT EDIT MANUALLY!!! ##########
########### THIS FILE IS GENERATED, DO NOT EDIT MANUALLY!!! ##########
######################################################################

from enum import Enum


class MetadataRecoveryStrategy(str, Enum):
    none = "none"
    rebootstrap = "rebootstrap"


class BrokerAddressFamily(str, Enum):
    any = "any"
    v4 = "v4"
    v6 = "v6"


class SecurityProtocol(str, Enum):
    plaintext = "plaintext"
    ssl = "ssl"
    sasl_plaintext = "sasl_plaintext"
    sasl_ssl = "sasl_ssl"


class SslEndpointIdentificationAlgorithm(str, Enum):
    none = "none"
    https = "https"


class SaslOauthbearerMethod(str, Enum):
    default = "default"
    oidc = "oidc"


class SaslOauthbearerGrantType(str, Enum):
    client_credentials = "client_credentials"
    jwt_bearer = "urn:ietf:params:oauth:grant-type:jwt-bearer"


class SaslOauthbearerAssertionAlgorithm(str, Enum):
    RS256 = "RS256"
    ES256 = "ES256"


class SaslOauthbearerMetadataAuthenticationType(str, Enum):
    none = "none"
    azure_imds = "azure_imds"


class ClientDnsLookup(str, Enum):
    use_all_dns_ips = "use_all_dns_ips"
    resolve_canonical_bootstrap_servers_only = "resolve_canonical_bootstrap_servers_only"


class GroupProtocol(str, Enum):
    classic = "classic"
    consumer = "consumer"


class IsolationLevel(str, Enum):
    read_uncommitted = "read_uncommitted"
    read_committed = "read_committed"


class AutoOffsetReset(str, Enum):
    smallest = "smallest"
    earliest = "earliest"
    beginning = "beginning"
    largest = "largest"
    latest = "latest"
    end = "end"
    error = "error"


class CompressionCodec(str, Enum):
    none = "none"
    gzip = "gzip"
    snappy = "snappy"
    lz4 = "lz4"
    zstd = "zstd"


class CompressionType(str, Enum):
    none = "none"
    gzip = "gzip"
    snappy = "snappy"
    lz4 = "lz4"
    zstd = "zstd"


class QueuingStrategy(str, Enum):
    fifo = "fifo"
    lifo = "lifo"
