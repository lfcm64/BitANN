const std = @import("std");
const types = @import("types.zig");

const mem = std.mem;
const testing = std.testing;

const Metadata = types.Metadata;
const Collection = types.Collection;
const Cluster = types.Cluster;
const Vector = types.Vector;

pub const RawPage = []u8;

pub const PageType = enum(u8) {
    metadata,
    collection,
    cluster,
    vector,
};

pub const PageState = enum {
    not_initialized,
    clean,
    dirty,
};

pub const PageHeader = packed struct {
    page_type: PageType,
    page_num: u32,
    next_page: u32,
};

pub const Page = struct {
    fn TypeToPage(comptime page_type: PageType) type {
        return switch (page_type) {
            .metadata => MetadataPage,
            .collection => CollectionPage,
            .cluster => ClusterPage,
            .vector => VectorPage,
        };
    }

    fn InitParameters(comptime page_type: PageType) type {
        return switch (page_type) {
            .metadata => Metadata,
            .collection => struct { page_num: u32 },
            .cluster => struct { page_num: u32, centroid_dim: u32 },
            .vector => struct { page_num: u32, vector_dim: u32 },
        };
    }

    page_num: u32,
    raw: RawPage,
    state: PageState,

    pub fn initialize(page: *Page, page_type: PageType, params: InitParameters(page_type)) !void {
        if (page.state != .not_initialized) return error.PageAlreadyInitialized;

        switch (page_type) {
            .metadata => MetadataPage.init(page.raw, params),
            .collection => CollectionPage.init(page.raw, params.page_num),
            .cluster => ClusterPage.init(page.raw, params.page_num, params.centroid_dim),
            .vector => VectorPage.init(page.raw, params.page_num, params.vector_dim),
        }
        page.state = .dirty;
    }

    pub fn header(page: *Page) !*PageHeader {
        if (page.state == .not_initialized) return error.PageNotInitialized;

        return @ptrCast(@alignCast(page.raw));
    }

    pub fn unwrap(page: *Page, comptime page_type: PageType) !*TypeToPage(page_type) {
        if (page.state == .not_initialized) return error.PageNotInitialized;
        if (page.raw[0] != @intFromEnum(page_type)) return error.WrongPageType;

        return switch (page_type) {
            .metadata => @as(*MetadataPage, @ptrCast(@alignCast(page.raw))),
            .collection => @as(*CollectionPage, @ptrCast(@alignCast(page.raw))),
            .cluster => @as(*ClusterPage, @ptrCast(@alignCast(page.raw))),
            .vector => @as(*VectorPage, @ptrCast(@alignCast(page.raw))),
        };
    }

    pub fn signal_change(page: *Page) void {
        page.state = .dirty;
    }
};

pub const MetadataPage = struct {
    header: PageHeader,
    meta: Metadata,

    pub fn init(raw: RawPage, meta: Metadata) void {
        const page: *MetadataPage = @ptrCast(@alignCast(raw));
        page.header = PageHeader{
            .page_type = .metadata,
            .page_num = 0,
            .next_page = 0,
        };
        page.meta = meta;
    }
};

pub const CollectionPage = packed struct {
    header: PageHeader,
    collection_count: u32,

    pub fn init(raw: RawPage, page_num: u32) void {
        const page: *CollectionPage = @ptrCast(@alignCast(raw));
        page.header = PageHeader{
            .page_type = .collection,
            .page_num = page_num,
            .next_page = 0,
        };
        page.collection_count = 0;
    }

    pub fn get_collection(page: *CollectionPage, index: u32) !Collection {
        if (index >= page.collection_count) return error.IndexOutOfBounds;

        const collections_data = page.get_collections_ptr();
        return collections_data[index];
    }

    pub fn add_collection(page: *CollectionPage, collection: Collection, page_size: u32) !void {
        if (page.collection_count >= page.max_collections(page_size)) {
            return error.PageFull;
        }

        const collections_data = page.get_collections_ptr();
        collections_data[page.collection_count] = collection;
        page.collection_count += 1;
    }

    pub fn find_collection(page: *CollectionPage, collection_id: u32) !Collection {
        const collections_data = page.get_collections_ptr();
        for (0..page.collection_count) |i| {
            if (collections_data[i].collection_id == collection_id) {
                return collections_data[i];
            }
        }
        return error.CollectionNotFound;
    }

    fn get_collections_ptr(page: *CollectionPage) [*]Collection {
        const page_bytes: [*]u8 = @ptrCast(page);
        const collections_offset = @sizeOf(CollectionPage);
        return @ptrCast(@alignCast(page_bytes + collections_offset));
    }

    fn max_collections(_: *CollectionPage, page_size: u32) u32 {
        const overhead = @sizeOf(CollectionPage);
        const collection_size = @sizeOf(Collection);
        return (page_size - overhead) / collection_size;
    }
};

test "initialization" {
    const page_size = 256;
    const buf = try testing.allocator.alloc(u8, page_size);
    defer testing.allocator.free(buf);

    @memset(buf, 1);

    CollectionPage.init(buf, 42);
    const page = @as(*CollectionPage, @ptrCast(@alignCast(buf)));

    try testing.expectEqual(PageType.collection, page.header.page_type);
    try testing.expectEqual(42, page.header.page_num);
    try testing.expectEqual(0, page.header.next_page);
    try testing.expectEqual(0, page.collection_count);

    try testing.expectEqual(@intFromEnum(PageType.collection), buf[0]);
    try testing.expectEqual(42, mem.readInt(u32, buf[1..5], .little));
    try testing.expectEqual(0, mem.readInt(u32, buf[5..9], .little));
    try testing.expectEqual(0, mem.readInt(u32, buf[9..13], .little));
}

test "field modification" {
    const page_size = 256;

    const buf = try testing.allocator.alloc(u8, page_size);
    defer testing.allocator.free(buf);

    @memset(buf, 1);

    CollectionPage.init(buf, 42);
    const page = @as(*CollectionPage, @ptrCast(@alignCast(buf)));

    try testing.expectEqual(0, page.header.next_page);
    try testing.expectEqual(0, mem.readInt(u32, buf[5..9], .little));

    try testing.expectEqual(0, page.collection_count);
    try testing.expectEqual(0, mem.readInt(u32, buf[9..13], .little));

    page.header.next_page = 16;
    page.collection_count = 8;

    try testing.expectEqual(16, page.header.next_page);
    try testing.expectEqual(16, mem.readInt(u32, buf[5..9], .little));

    try testing.expectEqual(8, page.collection_count);
    try testing.expectEqual(8, mem.readInt(u32, buf[9..13], .little));
}

test "get/add collection" {
    const page_size = 256;
    const buf = try testing.allocator.alloc(u8, page_size);
    defer testing.allocator.free(buf);

    @memset(buf, 0);

    CollectionPage.init(buf, 1);
    const page = @as(*CollectionPage, @ptrCast(@alignCast(buf)));

    try testing.expectError(error.IndexOutOfBounds, page.get_collection(0));

    // Create test collections
    const collection1 = Collection{ .id = 1, .first_cluster_page = 2, .vector_count = 3, .vector_dimension = 4 };
    const collection2 = Collection{ .id = 5, .first_cluster_page = 6, .vector_count = 7, .vector_dimension = 8 };

    // Add first collection
    try page.add_collection(collection1, page_size);
    try testing.expectEqual(1, page.collection_count);

    // Get and verify first collection
    const retrieved1 = try page.get_collection(0);
    try testing.expectEqualDeep(collection1, retrieved1);

    // Add second collection
    try page.add_collection(collection2, page_size);
    try testing.expectEqual(2, page.collection_count);

    // Get and verify second collections
    const retrieved2 = try page.get_collection(1);
    try testing.expectEqualDeep(collection2, retrieved2);
}

pub const ClusterPage = packed struct {
    header: PageHeader,
    cluster_count: u32,
    centroid_dim: u32,

    pub fn init(raw: RawPage, page_num: u32, centroid_dim: u32) void {
        const page: *ClusterPage = @ptrCast(@alignCast(raw));
        page.header = PageHeader{
            .page_type = .collection,
            .page_num = page_num,
            .next_page = 0,
        };
        page.cluster_count = 0;
        page.centroid_dim = centroid_dim;
    }

    pub fn get_cluster(page: *ClusterPage, index: u32) !Cluster {
        if (index >= page.cluster_count) return error.IndexOutOfBounds;
        const clusters_data = page.get_clusters_ptr();
        const cluster_size = @sizeOf(u32) + page.centroid_dim * @sizeOf(f32);
        const cluster_offset = index * cluster_size;

        const first_vector_page: *u32 = @ptrCast(@alignCast(clusters_data + cluster_offset));
        const vector_count: *u32 = @ptrCast(@alignCast(clusters_data + cluster_offset + @sizeOf(u32)));

        const centroid_ptr: [*]f32 = @ptrCast(@alignCast(clusters_data + cluster_offset + 2 * @sizeOf(u32)));
        const centroid = centroid_ptr[0..page.centroid_dim];

        return Cluster{
            .first_vector_page = first_vector_page.*,
            .vector_count = vector_count.*,
            .centroid = centroid,
        };
    }

    pub fn add_cluster(page: *ClusterPage, cluster: Cluster, page_size: u32) !void {
        if (page.cluster_count >= page.max_clusters(page_size)) {
            return error.PageFull;
        }
        if (cluster.centroid.len != page.centroid_dim) {
            return error.DimensionMismatch;
        }

        const clusters_data = page.get_clusters_ptr();
        const cluster_size = @sizeOf(u32) + page.centroid_dim * @sizeOf(f32);
        const cluster_offset = page.cluster_count * cluster_size;

        const first_page_ptr: *u32 = @ptrCast(@alignCast(clusters_data + cluster_offset));
        first_page_ptr.* = cluster.first_vector_page;

        const vector_count_ptr: *u32 = @ptrCast(@alignCast(clusters_data + cluster_offset + @sizeOf(u32)));
        vector_count_ptr.* = cluster.vector_count;

        const centroid_ptr: [*]f32 = @ptrCast(@alignCast(clusters_data + cluster_offset + 2 * @sizeOf(u32)));
        for (cluster.centroid, 0..) |val, i| {
            centroid_ptr[i] = val;
        }

        page.cluster_count += 1;
    }

    fn get_clusters_ptr(page: *ClusterPage) [*]u8 {
        const page_bytes: [*]u8 = @ptrCast(page);
        const clusters_offset = @sizeOf(ClusterPage);
        return page_bytes + clusters_offset;
    }

    fn max_clusters(page: *ClusterPage, page_size: u32) u32 {
        const overhead = @sizeOf(ClusterPage);
        const cluster_size = @sizeOf(u32) + page.centroid_dim * @sizeOf(f32);
        return (page_size - overhead) / cluster_size;
    }
};

test "get/add cluster" {
    const page_size = 256;
    const buf = try testing.allocator.alloc(u8, page_size);
    defer testing.allocator.free(buf);
    @memset(buf, 0);

    ClusterPage.init(buf, 1, 3);
    const page = @as(*ClusterPage, @ptrCast(@alignCast(buf)));

    try testing.expectError(error.IndexOutOfBounds, page.get_cluster(0));

    // Create test clusters
    var centroid1 = [_]f32{ 1.0, 2.0, 3.0 };
    var centroid2 = [_]f32{ 4.0, 5.0, 6.0 };

    const cluster1 = Cluster{ .first_vector_page = 1, .vector_count = 2, .centroid = centroid1[0..] };
    const cluster2 = Cluster{ .first_vector_page = 3, .vector_count = 4, .centroid = centroid2[0..] };

    // Add first cluster
    try page.add_cluster(cluster1, page_size);
    try testing.expectEqual(1, page.cluster_count);

    // Get and verify first cluster
    const retrieved1 = try page.get_cluster(0);
    try testing.expectEqualDeep(cluster1, retrieved1);

    // Add second cluster
    try page.add_cluster(cluster2, page_size);
    try testing.expectEqual(2, page.cluster_count);

    // Get and verify second cluster
    const retrieved2 = try page.get_cluster(1);
    try testing.expectEqualDeep(cluster2, retrieved2);
}

pub const VectorPage = packed struct {
    header: PageHeader,
    vector_count: u32,
    vector_dim: u32,

    pub fn init(raw: RawPage, page_num: u32, vector_dim: u32) void {
        const page: *VectorPage = @ptrCast(@alignCast(raw));
        page.header = PageHeader{
            .page_type = .collection,
            .page_num = page_num,
            .next_page = 0,
        };
        page.vector_count = 0;
        page.vector_dim = vector_dim;
    }

    pub fn get_vector(page: *VectorPage, index: u32) !Vector {
        if (index >= page.vector_count) return error.IndexOutOfBounds;
        const vectors_data = page.get_vectors_ptr();
        const vector_size = @sizeOf(u32) + page.vector_dim * @sizeOf(f32);
        const vector_offset = index * vector_size;

        const id_ptr: *u32 = @ptrCast(@alignCast(vectors_data + vector_offset));
        const point_ptr: [*]f32 = @ptrCast(@alignCast(vectors_data + vector_offset + @sizeOf(u32)));
        const point = point_ptr[0..page.vector_dim];

        return Vector{
            .id = id_ptr.*,
            .point = point,
        };
    }

    pub fn add_vector(page: *VectorPage, vector: Vector, page_size: u32) !void {
        if (page.vector_count >= page.max_vectors(page_size)) {
            return error.PageFull;
        }
        if (vector.point.len != page.vector_dim) {
            return error.DimensionMismatch;
        }

        const vectors_data = page.get_vectors_ptr();
        const vector_size = @sizeOf(u32) + page.vector_dim * @sizeOf(f32);
        const vector_offset = page.vector_count * vector_size;

        const id_ptr: *u32 = @ptrCast(@alignCast(vectors_data + vector_offset));
        id_ptr.* = vector.id;

        const point_ptr: [*]f32 = @ptrCast(@alignCast(vectors_data + vector_offset + @sizeOf(u32)));
        for (vector.point, 0..) |val, i| {
            point_ptr[i] = val;
        }

        page.vector_count += 1;
    }

    fn get_vectors_ptr(page: *VectorPage) [*]u8 {
        const page_bytes: [*]u8 = @ptrCast(page);
        const vectors_offset = @sizeOf(VectorPage);
        return page_bytes + vectors_offset;
    }

    fn max_vectors(page: *VectorPage, page_size: u32) u32 {
        const overhead = @sizeOf(VectorPage);
        const vector_size = @sizeOf(u32) + page.vector_dim * @sizeOf(f32);
        return (page_size - overhead) / vector_size;
    }
};

test "get/add vector" {
    const page_size = 256;
    const buf = try testing.allocator.alloc(u8, page_size);
    defer testing.allocator.free(buf);
    @memset(buf, 0);

    VectorPage.init(buf, 1, 3);
    const page = @as(*VectorPage, @ptrCast(@alignCast(buf)));

    try testing.expectError(error.IndexOutOfBounds, page.get_vector(0));

    // Create test clusters
    var point1 = [_]f32{ 1.0, 2.0, 3.0 };
    var point2 = [_]f32{ 4.0, 5.0, 6.0 };

    const vector1 = Vector{ .id = 1, .point = &point1 };
    const vector2 = Vector{ .id = 3, .point = &point2 };

    // Add first cluster
    try page.add_vector(vector1, page_size);
    try testing.expectEqual(1, page.vector_count);

    // Get and verify first cluster
    const retrieved1 = try page.get_vector(0);
    try testing.expectEqualDeep(vector1, retrieved1);

    // Add second cluster
    try page.add_vector(vector2, page_size);
    try testing.expectEqual(2, page.vector_count);

    // Get and verify second cluster
    const retrieved2 = try page.get_vector(1);
    try testing.expectEqualDeep(vector2, retrieved2);
}
