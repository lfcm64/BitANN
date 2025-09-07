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

pub const Index = union(enum) {
    flat: packed struct {
        first_vector_page: u32,
    },

    pub fn eql(a: Index, b: Index) bool {
        return switch (a) {
            .flat => |a_flat| switch (b) {
                .flat => |b_flat| a_flat.first_vector_page == b_flat.first_vector_page,
            },
        };
    }
};

pub const Quantization = union(enum) {
    none: packed struct {
        dimension: u32,
    },

    pub fn data_size(quant: Quantization) usize {
        return switch (quant) {
            .none => |none| none.dimension * @sizeOf(f32),
        };
    }

    pub fn eql(a: Quantization, b: Quantization) bool {
        return switch (a) {
            .none => |a_none| switch (b) {
                .none => |b_none| a_none.dimension == b_none.dimension,
            },
        };
    }
};

pub const ItemType = enum {
    collection,
    cluster,
    vector,
};

pub fn Item(comptime item_type: ItemType) type {
    return switch (item_type) {
        .collection => Collection,
        .cluster => Cluster,
        .vector => Vector,
    };
}

pub const Collection = struct {
    id: u32,
    vector_count: u32,
    index: Index,
    quantization: Quantization,

    pub fn eql(a: Collection, b: Collection) bool {
        return a.id == b.id and
            a.vector_count == b.vector_count and
            a.index.eql(b.index) and
            a.quantization.eql(b.quantization);
    }
};

pub const Cluster = struct {
    first_vector_page: u32,
    vector_count: u32,
    quantization: Quantization,
    position: []u8,

    pub fn eql(a: Cluster, b: Cluster) bool {
        if (a.first_vector_page != b.first_vector_page or a.vector_count != b.vector_count) {
            return false;
        }
        if (!a.quantization.eql(b.quantization)) {
            return false;
        }
        return std.mem.eql(u8, a.position, b.position);
    }
};

pub const Vector = struct {
    id: u32,
    quantization: Quantization,
    position: []u8,

    pub fn eql(a: Vector, b: Vector) bool {
        if (a.id != b.id) {
            return false;
        }
        if (!a.quantization.eql(b.quantization)) {
            return false;
        }
        return std.mem.eql(u8, a.position, b.position);
    }
};
