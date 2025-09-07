const std = @import("std");
const types = @import("types.zig");

const mem = std.mem;
const testing = std.testing;

const Metadata = types.Metadata;
const Index = types.Index;
const Quantization = types.Quantization;
const Item = types.Item;
const ItemType = types.ItemType;
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
    free,
    clean,
    dirty,
};

pub const PageHeader = packed struct {
    page_type: PageType,
    page_num: u32,
    prev_page: u32,
    next_page: u32,
};

pub fn PageParams(comptime page_type: PageType) type {
    return switch (page_type) {
        .metadata => MetadataPageParams,
        .collection => CollectionPageParams,
        .cluster => ClusterPageParams,
        .vector => VectorPageParams,
    };
}

pub fn Page(comptime page_type: PageType) type {
    return switch (page_type) {
        .metadata => MetadataPage,
        .collection => CollectionPage,
        .cluster => ClusterPage,
        .vector => VectorPage,
    };
}

pub fn ItemPage(comptime item_type: ItemType) type {
    return struct {
        const IPage = @This();

        ptr: *anyopaque,
        vtable: *const Vtable,

        const Vtable = struct {
            get: *const fn (*anyopaque, index: u32) ?Item(item_type),
            insert: *const fn (*anyopaque, index: u32, item: Item(item_type)) anyerror!void,
            update: *const fn (*anyopaque, index: u32, item: Item(item_type)) anyerror!void,
            is_full: *const fn (*anyopaque) bool,
        };

        pub fn get(page: IPage, index: u32) ?Item(item_type) {
            return page.vtable.get(page.ptr, index);
        }

        pub fn insert(page: IPage, index: u32, item: Item(item_type)) anyerror!void {
            return page.vtable.insert(page.ptr, index, item);
        }

        pub fn update(page: IPage, index: u32, item: Item(item_type)) anyerror!void {
            return page.vtable.update(page.ptr, index, item);
        }

        pub fn is_full(page: IPage) bool {
            return page.vtable.is_full(page.ptr);
        }
    };
}

pub const MetadataPageParams = Metadata;

pub const MetadataPage = struct {
    header: PageHeader,
    meta: Metadata,

    pub fn init(raw: RawPage) !*MetadataPage {
        return @ptrCast(@alignCast(raw));
    }

    pub fn create(raw: RawPage, params: MetadataPageParams) *MetadataPage {
        const page: *MetadataPage = @ptrCast(@alignCast(raw));
        page.header = PageHeader{
            .page_type = .metadata,
            .page_num = 0,
            .prev_page = 0,
            .next_page = 0,
        };
        page.meta = params;
        return page;
    }
};

pub const CollectionPageParams = struct {
    prev_page: u32,
};

pub const CollectionPage = packed struct {
    header: PageHeader,
    slots: u32,
    collection_count: u32,

    pub const CollectionSlot = struct {
        tombstone: bool,
        collection: Collection,
    };

    pub fn create(raw: RawPage, page_num: u32, params: CollectionPageParams) *CollectionPage {
        @memset(raw, 0);
        const page: *CollectionPage = @ptrCast(@alignCast(raw));
        page.header = PageHeader{
            .page_type = .collection,
            .page_num = page_num,
            .prev_page = params.prev_page,
            .next_page = 0,
        };
        page.slots = @intCast((raw.len - @sizeOf(CollectionPage)) / @sizeOf(CollectionSlot));
        page.collection_count = 0;
        return page;
    }

    pub fn item_page(page: *CollectionPage) ItemPage(.collection) {
        return ItemPage(.collection){
            .ptr = page,
            .vtable = &.{
                .get = get_collection_erased,
                .insert = insert_collection_erased,
                .update = update_collection_erased,
                .is_full = is_full_erased,
            },
        };
    }

    pub fn get_collection(page: *CollectionPage, index: u32) ?Collection {
        if (index >= page.collection_count) return null;
        const collections = page.get_collections_ptr();
        if (!collections[index].tombstone) return null;
        return collections[index].collection;
    }

    fn get_collection_erased(ptr: *anyopaque, index: u32) ?Collection {
        const page: *CollectionPage = @ptrCast(@alignCast(ptr));
        return page.get_collection(index);
    }

    pub fn update_collection(page: *CollectionPage, index: u32, collection: Collection) !void {
        if (index >= page.collection_count) return error.IndexOutOfBounds;
        const collections = page.get_collections_ptr();
        if (!collections[index].tombstone) return error.EmptySlot;
        collections[index].collection = collection;
    }

    fn update_collection_erased(ptr: *anyopaque, index: u32, collection: Collection) anyerror!void {
        const page: *CollectionPage = @ptrCast(@alignCast(ptr));
        return page.update_collection(index, collection);
    }

    pub fn insert_collection(page: *CollectionPage, index: u32, collection: Collection) !void {
        if (index >= page.slots) return error.IndexOutOfBounds;
        const collections = page.get_collections_ptr();
        if (collections[index].tombstone) return error.CollectionAlreadyExists;
        collections[index] = CollectionSlot{
            .tombstone = true,
            .collection = collection,
        };
        page.collection_count += 1;
    }

    fn insert_collection_erased(ptr: *anyopaque, index: u32, collection: Collection) anyerror!void {
        const page: *CollectionPage = @ptrCast(@alignCast(ptr));
        return page.insert_collection(index, collection);
    }

    pub fn is_full(page: *CollectionPage) bool {
        return page.slots <= page.collection_count;
    }

    fn is_full_erased(ptr: *anyopaque) bool {
        const page: *CollectionPage = @ptrCast(@alignCast(ptr));
        return page.is_full();
    }

    fn get_collections_ptr(page: *CollectionPage) [*]CollectionSlot {
        const page_bytes: [*]u8 = @ptrCast(page);
        const collections_offset = @sizeOf(CollectionPage);
        return @ptrCast(@alignCast(page_bytes + collections_offset));
    }
};

test "initialization" {
    const page_size = 256;
    const buf = try testing.allocator.alloc(u8, page_size);
    defer testing.allocator.free(buf);

    const page = CollectionPage.create(buf, 42, .{ .prev_page = 24 });
    const max_slots = (page_size - @sizeOf(CollectionPage)) / @sizeOf(CollectionPage.CollectionSlot);

    try testing.expectEqual(PageType.collection, page.header.page_type);
    try testing.expectEqual(42, page.header.page_num);
    try testing.expectEqual(24, page.header.prev_page);
    try testing.expectEqual(0, page.header.next_page);
    try testing.expectEqual(max_slots, page.slots);
    try testing.expectEqual(0, page.collection_count);

    try testing.expectEqual(@intFromEnum(PageType.collection), buf[0]);
    try testing.expectEqual(42, mem.readInt(u32, buf[1..5], .little));
    try testing.expectEqual(24, mem.readInt(u32, buf[5..9], .little));
    try testing.expectEqual(0, mem.readInt(u32, buf[9..13], .little));
    try testing.expectEqual(max_slots, mem.readInt(u32, buf[13..17], .little));
    try testing.expectEqual(0, mem.readInt(u32, buf[17..21], .little));
}

test "field modification" {
    const page_size = 256;
    const buf = try testing.allocator.alloc(u8, page_size);
    defer testing.allocator.free(buf);

    const page = CollectionPage.create(buf, 42, .{ .prev_page = 24 });

    try testing.expectEqual(0, page.header.next_page);
    try testing.expectEqual(0, mem.readInt(u32, buf[9..13], .little));
    try testing.expectEqual(0, page.collection_count);
    try testing.expectEqual(0, mem.readInt(u32, buf[17..21], .little));

    page.header.next_page = 16;
    page.collection_count = 8;

    try testing.expectEqual(16, page.header.next_page);
    try testing.expectEqual(16, mem.readInt(u32, buf[9..13], .little));
    try testing.expectEqual(8, page.collection_count);
    try testing.expectEqual(8, mem.readInt(u32, buf[17..21], .little));
}

test "get/insert collection" {
    const page_size = 256;
    const buf = try testing.allocator.alloc(u8, page_size);
    defer testing.allocator.free(buf);

    const page = CollectionPage.create(buf, 42, .{ .prev_page = 24 });

    try testing.expect(page.get_collection(0) == null);

    const index1 = Index{ .flat = .{ .first_vector_page = 2 } };
    const index2 = Index{ .flat = .{ .first_vector_page = 7 } };

    const quant1 = Quantization{ .none = .{ .dimension = 4 } };
    const quant2 = Quantization{ .none = .{ .dimension = 8 } };

    const collection1 = Collection{ .id = 1, .vector_count = 100, .index = index1, .quantization = quant1 };
    const collection2 = Collection{ .id = 5, .vector_count = 24, .index = index2, .quantization = quant2 };

    try page.insert_collection(0, collection1);
    try testing.expectEqual(1, page.collection_count);

    const retrieved1 = page.get_collection(0);
    try testing.expect(collection1.eql(retrieved1.?));

    try page.insert_collection(1, collection2);
    try testing.expectEqual(2, page.collection_count);

    const retrieved2 = page.get_collection(1);
    try testing.expect(collection2.eql(retrieved2.?));
}

test "update collection" {
    const page_size = 256;
    const buf = try testing.allocator.alloc(u8, page_size);
    defer testing.allocator.free(buf);

    const page = CollectionPage.create(buf, 42, .{ .prev_page = 24 });

    const original_index = Index{ .flat = .{ .first_vector_page = 2 } };
    const updated_index = Index{ .flat = .{ .first_vector_page = 10 } };

    const original_quant = Quantization{ .none = .{ .dimension = 4 } };
    const updated_quant = Quantization{ .none = .{ .dimension = 6 } };

    const original_collection = Collection{
        .id = 1,
        .vector_count = 3,
        .index = original_index,
        .quantization = original_quant,
    };
    const updated_collection = Collection{
        .id = 1,
        .vector_count = 20,
        .index = updated_index,
        .quantization = updated_quant,
    };

    try page.insert_collection(0, original_collection);

    const retrieved = page.get_collection(0);
    try testing.expect(original_collection.eql(retrieved.?));

    try page.update_collection(0, updated_collection);

    const updated_retrieved = page.get_collection(0);
    try testing.expect(updated_collection.eql(updated_retrieved.?));

    try testing.expectError(error.IndexOutOfBounds, page.update_collection(999, updated_collection));

    const collections = page.get_collections_ptr();
    collections[0].tombstone = false;
    try testing.expectError(error.EmptySlot, page.update_collection(0, updated_collection));
}

pub const ClusterPageParams = struct {
    prev_page: u32,
    quantization: Quantization,
};

pub const ClusterPage = struct {
    header: PageHeader,
    slots: u32,
    cluster_count: u32,
    quantization: Quantization,

    pub const ClusterInfo = packed struct {
        tombstone: bool,
        first_vector_page: u32,
        vector_count: u32,
    };

    pub fn create(raw: RawPage, page_num: u32, params: ClusterPageParams) *ClusterPage {
        @memset(raw, 0);
        const page: *ClusterPage = @ptrCast(@alignCast(raw));
        page.header = PageHeader{
            .page_type = .cluster,
            .page_num = page_num,
            .prev_page = params.prev_page,
            .next_page = 0,
        };
        page.slots = @intCast((raw.len - @sizeOf(ClusterPage)) / (@sizeOf(ClusterInfo) + params.quantization.data_size()));
        page.cluster_count = 0;
        page.quantization = params.quantization;
        return page;
    }

    pub fn item_page(page: *ClusterPage) ItemPage(.cluster) {
        return ItemPage(.cluster){
            .ptr = page,
            .vtable = &.{
                .get = get_cluster_erased,
                .insert = insert_cluster_erased,
                .update = update_cluster_erased,
                .is_full = is_full_erased,
            },
        };
    }

    pub fn get_cluster(page: *ClusterPage, index: u32) ?Cluster {
        if (index >= page.slots) return null;

        const cluster_infos = page.get_cluster_infos();
        if (!cluster_infos[index].tombstone) return null;

        return Cluster{
            .first_vector_page = cluster_infos[index].first_vector_page,
            .vector_count = cluster_infos[index].vector_count,
            .quantization = page.quantization,
            .position = page.get_position_data(index),
        };
    }

    fn get_cluster_erased(ptr: *anyopaque, index: u32) ?Cluster {
        const page: *ClusterPage = @ptrCast(@alignCast(ptr));
        return page.get_cluster(index);
    }

    pub fn insert_cluster(page: *ClusterPage, index: u32, cluster: Cluster) !void {
        if (index >= page.slots) return error.IndexOutOfBounds;
        if (!cluster.quantization.eql(page.quantization)) return error.QuantizationMismatch;
        if (page.quantization.data_size() != cluster.position.len) return error.BadPositionFormat;

        const cluster_infos = page.get_cluster_infos();
        if (cluster_infos[index].tombstone) return error.ClusterAlreadyExists;

        cluster_infos[index] = ClusterInfo{
            .tombstone = true,
            .first_vector_page = cluster.first_vector_page,
            .vector_count = cluster.vector_count,
        };
        const position_data = page.get_position_data(index);
        @memcpy(position_data, cluster.position);
        page.cluster_count = index + 1;
    }

    fn insert_cluster_erased(ptr: *anyopaque, index: u32, cluster: Cluster) anyerror!void {
        const page: *ClusterPage = @ptrCast(@alignCast(ptr));
        return page.insert_cluster(index, cluster);
    }

    pub fn update_cluster(page: *ClusterPage, index: u32, cluster: Cluster) !void {
        if (index >= page.slots) return error.IndexOutOfBounds;
        if (!cluster.quantization.eql(page.quantization)) return error.QuantizationMismatch;
        if (page.quantization.data_size() != cluster.position.len) return error.BadPositionFormat;

        const cluster_infos = page.get_cluster_infos();
        if (!cluster_infos[index].tombstone) return error.EmptySlot;

        cluster_infos[index].first_vector_page = cluster.first_vector_page;
        cluster_infos[index].vector_count = cluster.vector_count;
        const position_data = page.get_position_data(index);
        @memcpy(position_data, cluster.position);
    }

    fn update_cluster_erased(ptr: *anyopaque, index: u32, cluster: Cluster) anyerror!void {
        const page: *ClusterPage = @ptrCast(@alignCast(ptr));
        return page.update_cluster(index, cluster);
    }

    pub fn is_full(page: *ClusterPage) bool {
        return page.slots <= page.cluster_count;
    }

    fn is_full_erased(ptr: *anyopaque) bool {
        const page: *ClusterPage = @ptrCast(@alignCast(ptr));
        return page.is_full();
    }

    fn get_cluster_infos(page: *ClusterPage) []ClusterInfo {
        const page_bytes: [*]u8 = @ptrCast(page);
        const infos_offset = @sizeOf(ClusterPage);
        const infos_ptr: [*]ClusterInfo = @ptrCast(@alignCast(page_bytes + infos_offset));
        return infos_ptr[0..page.slots];
    }

    fn get_position_data(page: *ClusterPage, index: u32) []u8 {
        const page_bytes: [*]u8 = @ptrCast(page);
        const infos_size = @sizeOf(ClusterInfo) * page.slots;
        const data_start_offset = @sizeOf(ClusterPage) + infos_size;
        const position_size = page.quantization.data_size();
        const position_offset = data_start_offset + (index * position_size);
        return page_bytes[position_offset..][0..position_size];
    }
};

test "cluster page get/insert" {
    const page_size = 256;
    const buf = try testing.allocator.alloc(u8, page_size);
    defer testing.allocator.free(buf);

    const quant = Quantization{ .none = .{ .dimension = 3 } };

    const page = ClusterPage.create(buf, 10, .{ .prev_page = 0, .quantization = quant });
    try testing.expectEqual(null, page.get_cluster(0));

    var position1 = [_]u8{ 0x00, 0x00, 0x80, 0x3F, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40 }; // 1.0, 2.0, 3.0 as bytes
    var position2 = [_]u8{ 0x00, 0x00, 0x80, 0x40, 0x00, 0x00, 0xA0, 0x40, 0x00, 0x00, 0xC0, 0x40 }; // 4.0, 5.0, 6.0 as bytes

    const cluster1 = Cluster{ .first_vector_page = 1, .vector_count = 2, .position = position1[0..], .quantization = quant };
    const cluster2 = Cluster{ .first_vector_page = 3, .vector_count = 4, .position = position2[0..], .quantization = quant };

    try page.insert_cluster(0, cluster1);
    try testing.expectEqual(1, page.cluster_count);

    const retrieved1 = page.get_cluster(0).?;
    try testing.expect(retrieved1.eql(cluster1));

    try page.insert_cluster(1, cluster2);
    try testing.expectEqual(2, page.cluster_count);

    const retrieved2 = page.get_cluster(1).?;
    try testing.expect(retrieved2.eql(cluster2));
}

test "cluster page update" {
    const page_size = 256;
    const buf = try testing.allocator.alloc(u8, page_size);
    defer testing.allocator.free(buf);
    @memset(buf, 0);

    const quant = Quantization{ .none = .{ .dimension = 3 } };

    const page = ClusterPage.create(buf, 10, .{ .prev_page = 0, .quantization = quant });

    var original_position = [_]u8{ 0x00, 0x00, 0x80, 0x3F, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40 }; // 1.0, 2.0, 3.0
    var updated_position = [_]u8{ 0x00, 0x00, 0x20, 0x41, 0x00, 0x00, 0xA0, 0x41, 0x00, 0x00, 0xF0, 0x41 }; // 10.0, 20.0, 30.0

    const original_cluster = Cluster{
        .first_vector_page = 1,
        .vector_count = 2,
        .position = original_position[0..],
        .quantization = quant,
    };
    const updated_cluster = Cluster{
        .first_vector_page = 100,
        .vector_count = 200,
        .position = updated_position[0..],
        .quantization = quant,
    };

    try page.insert_cluster(0, original_cluster);
    try page.update_cluster(0, updated_cluster);

    const retrieved = page.get_cluster(0).?;
    try testing.expect(retrieved.eql(updated_cluster));

    try testing.expectError(error.IndexOutOfBounds, page.update_cluster(999, updated_cluster));
    const cluster_infos = page.get_cluster_infos();
    cluster_infos[0].tombstone = false;
    try testing.expectError(error.EmptySlot, page.update_cluster(0, updated_cluster));
}

pub const VectorPageParams = struct {
    prev_page: u32,
    quantization: Quantization,
};

pub const VectorPage = struct {
    header: PageHeader,
    slots: u32,
    vector_count: u32,
    quantization: Quantization,

    pub const VectorInfo = packed struct {
        tombstone: bool,
        id: u32,
    };

    pub fn create(raw: RawPage, page_num: u32, params: VectorPageParams) *VectorPage {
        @memset(raw, 0);
        const page: *VectorPage = @ptrCast(@alignCast(raw));
        page.header = PageHeader{
            .page_type = .vector,
            .page_num = page_num,
            .prev_page = params.prev_page,
            .next_page = 0,
        };
        page.slots = @intCast((raw.len - @sizeOf(VectorPage)) / (@sizeOf(VectorInfo) + params.quantization.data_size()));
        page.vector_count = 0;
        page.quantization = params.quantization;
        return page;
    }

    pub fn item_page(page: *VectorPage) ItemPage(.vector) {
        return ItemPage(.vector){
            .ptr = page,
            .vtable = &.{
                .get = get_vector_erased,
                .insert = insert_vector_erased,
                .update = update_vector_erased,
                .is_full = is_full_erased,
            },
        };
    }

    pub fn get_vector(page: *VectorPage, index: u32) ?Vector {
        if (index >= page.slots) return null;

        const vector_infos = page.get_vector_infos();
        if (!vector_infos[index].tombstone) return null;

        return Vector{
            .id = vector_infos[index].id,
            .quantization = page.quantization,
            .position = page.get_position_data(index),
        };
    }

    fn get_vector_erased(ptr: *anyopaque, index: u32) ?Vector {
        const page: *VectorPage = @ptrCast(@alignCast(ptr));
        return page.get_vector(index);
    }

    pub fn insert_vector(page: *VectorPage, index: u32, vector: Vector) !void {
        if (index >= page.slots) return error.IndexOutOfBounds;
        if (!vector.quantization.eql(page.quantization)) return error.QuantizationMismatch;
        if (page.quantization.data_size() != vector.position.len) return error.BadPositionFormat;

        const vector_infos = page.get_vector_infos();
        if (vector_infos[index].tombstone) return error.VectorAlreadyExists;

        vector_infos[index] = VectorInfo{
            .tombstone = true,
            .id = vector.id,
        };
        const position_data = page.get_position_data(index);
        @memcpy(position_data, vector.position);
        page.vector_count = index + 1;
    }

    fn insert_vector_erased(ptr: *anyopaque, index: u32, vector: Vector) anyerror!void {
        const page: *VectorPage = @ptrCast(@alignCast(ptr));
        return page.insert_vector(index, vector);
    }

    pub fn update_vector(page: *VectorPage, index: u32, vector: Vector) !void {
        if (index >= page.slots) return error.IndexOutOfBounds;
        if (!vector.quantization.eql(page.quantization)) return error.QuantizationMismatch;
        if (page.quantization.data_size() != vector.position.len) return error.BadPositionFormat;

        const vector_infos = page.get_vector_infos();
        if (!vector_infos[index].tombstone) return error.EmptySlot;

        vector_infos[index].id = vector.id;
        const position_data = page.get_position_data(index);
        @memcpy(position_data, vector.position);
    }

    fn update_vector_erased(ptr: *anyopaque, index: u32, vector: Vector) anyerror!void {
        const page: *VectorPage = @ptrCast(@alignCast(ptr));
        return page.update_vector(index, vector);
    }

    pub fn is_full(page: *VectorPage) bool {
        return page.slots <= page.vector_count;
    }

    fn is_full_erased(ptr: *anyopaque) bool {
        const page: *VectorPage = @ptrCast(@alignCast(ptr));
        return page.is_full();
    }

    fn get_vector_infos(page: *VectorPage) []VectorInfo {
        const page_bytes: [*]u8 = @ptrCast(page);
        const infos_offset = @sizeOf(VectorPage);
        const infos_ptr: [*]VectorInfo = @ptrCast(@alignCast(page_bytes + infos_offset));
        return infos_ptr[0..page.slots];
    }

    fn get_position_data(page: *VectorPage, index: u32) []u8 {
        const page_bytes: [*]u8 = @ptrCast(page);
        const infos_size = @sizeOf(VectorInfo) * page.slots;
        const data_start_offset = @sizeOf(VectorPage) + infos_size;
        const position_size = page.quantization.data_size();
        const position_offset = data_start_offset + (index * position_size);
        return page_bytes[position_offset..][0..position_size];
    }
};

test "vector page get/insert" {
    const page_size = 256;
    const buf = try testing.allocator.alloc(u8, page_size);
    defer testing.allocator.free(buf);

    const quant = Quantization{ .none = .{ .dimension = 3 } };

    const page = VectorPage.create(buf, 5, .{ .prev_page = 0, .quantization = quant });
    try testing.expect(page.get_vector(0) == null);

    var position1 = [_]u8{ 0x00, 0x00, 0x80, 0x3F, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40 }; // 1.0, 2.0, 3.0 as bytes
    var position2 = [_]u8{ 0x00, 0x00, 0x80, 0x40, 0x00, 0x00, 0xA0, 0x40, 0x00, 0x00, 0xC0, 0x40 }; // 4.0, 5.0, 6.0 as bytes
    const vector1 = Vector{ .id = 10, .position = position1[0..], .quantization = quant };
    const vector2 = Vector{ .id = 20, .position = position2[0..], .quantization = quant };

    try page.insert_vector(0, vector1);
    try testing.expectEqual(1, page.vector_count);

    const retrieved1 = page.get_vector(0).?;
    try testing.expect(retrieved1.eql(vector1));

    try page.insert_vector(1, vector2);
    try testing.expectEqual(2, page.vector_count);

    const retrieved2 = page.get_vector(1).?;
    try testing.expect(retrieved2.eql(vector2));
}

test "vector page update" {
    const page_size = 256;
    const buf = try testing.allocator.alloc(u8, page_size);
    defer testing.allocator.free(buf);
    @memset(buf, 0);

    const quant = Quantization{ .none = .{ .dimension = 3 } };

    const page = VectorPage.create(buf, 5, .{ .prev_page = 0, .quantization = quant });

    var original_position = [_]u8{ 0x00, 0x00, 0x80, 0x3F, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40 }; // 1.0, 2.0, 3.0
    var updated_position = [_]u8{ 0x00, 0x00, 0x20, 0x41, 0x00, 0x00, 0xA0, 0x41, 0x00, 0x00, 0xF0, 0x41 }; // 10.0, 20.0, 30.0
    const original_vector = Vector{ .id = 100, .position = original_position[0..], .quantization = quant };
    const updated_vector = Vector{ .id = 200, .position = updated_position[0..], .quantization = quant };

    try page.insert_vector(0, original_vector);
    try page.update_vector(0, updated_vector);

    const retrieved = page.get_vector(0).?;
    try testing.expect(retrieved.eql(updated_vector));

    try testing.expectError(error.IndexOutOfBounds, page.update_vector(999, updated_vector));
    const vector_infos = page.get_vector_infos();
    vector_infos[0].tombstone = false;
    try testing.expectError(error.EmptySlot, page.update_vector(0, updated_vector));
}
