const PageType = enum {
    CLUSTERS,
    VECTORS,
    PAYLOADS,
};

const ClusterPage = struct {
    raw: []u8,
};

const VectorPage = struct {
    raw: []u8,
};

const PayloadPage = struct {
    raw: []u8,
};

const Page = union(PageType) {
    CLUSTERS: ClusterPage,
    VECTORS: VectorPage,
    PAYLOADS: PayloadPage,
};
