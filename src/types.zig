const std = @import("std");

pub const Metadata = struct {
    magic: [6]u8 = [6]u8{ 'a', 'b', 'c', 'd', 'e', 'f' },
    version: u16 = 1,
    page_size: u32 = 4096,
    page_count: u32 = 1,
    first_collection_page: u32 = 1,
    free_list_start: u32 = 0,
    cache_size: u32 = 1024,

    pub fn validate(_: *Metadata) void {}
};

pub const IndexType = enum(u8) {
    flat,
    ivf,
};

pub const QuantizationType = enum(u8) {
    none,
};

pub const ItemType = enum {
    collection,
    cluster,
    vector,
};

pub fn Item(comptime item_type: ItemType) type {
    return switch (item_type) {
        .collection => StoredCollection,
        .cluster => StoredCluster,
        .vector => StoredVector,
    };
}

pub const StoredCollection = packed struct {
    id: u32,
    dimensions: u32,
    quant: QuantizationType,
    index: IndexType,
    first_child_page: u32,

    pub fn serialize(collection: *const StoredCollection, buf: []u8) !void {
        var stream = std.io.fixedBufferStream(buf);
        var writer = stream.writer();

        try writer.writeInt(u32, collection.id, .little);
        try writer.writeInt(u32, collection.dimensions, .little);
        try writer.writeInt(u8, @intFromEnum(collection.quant), .little);
        try writer.writeInt(u32, collection.first_child_page, .little);
    }

    pub fn deserialize(buf: []const u8) StoredCollection {
        return StoredCollection{
            .id = std.mem.readInt(u32, buf[0..4], .little),
            .dimensions = std.mem.readInt(u32, buf[4..8], .little),
            .quant = @enumFromInt(std.mem.readInt(u8, buf[8..9], .little)),
            .index = IndexType.flat, // Placeholder, to be implemented
            .first_child_page = std.mem.readInt(u32, buf[9..13], .little),
        };
    }
};

pub const StoredCluster = struct {
    first_child_page: u32,
    position_data: []const u8,

    pub fn serialize(cluster: *const StoredCluster, buf: []u8) !void {
        var stream = std.io.fixedBufferStream(buf);
        var writer = stream.writer();

        try writer.writeInt(u32, cluster.first_vector_page, .little);
        try writer.writeAll(cluster.position_data);
    }

    pub fn deserialize(buf: []const u8) StoredCluster {
        return StoredCluster{
            .first_vector_page = std.mem.readInt(u32, buf[0..4], .little),
            .position_data = buf[4..],
        };
    }
};

pub const Vector = struct {
    id: u32,
    position: []f32,
};

pub const StoredVector = struct {
    id: u32,
    position_data: []const u8,

    pub fn serialize(vec: *const StoredVector, buf: []u8) !void {
        var stream = std.io.fixedBufferStream(buf);
        var writer = stream.writer();

        try writer.writeInt(u32, vec.id, .little);
        try writer.writeAll(vec.position_data);
    }

    pub fn deserialize(buf: []const u8) StoredVector {
        return StoredVector{
            .id = std.mem.readInt(u32, buf[0..4], .little),
            .position_data = buf[4..],
        };
    }
};
