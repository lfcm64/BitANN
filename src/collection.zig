const std = @import("std");
const types = @import("types.zig");
const indexes = @import("indexes.zig");
const quantizers = @import("quantizers.zig");

const Db = @import("db.zig").Db;

const Allocator = std.mem.Allocator;

const VectorIndex = indexes.VectorIndex;
const Quantizer = quantizers.Quantizer;

const Vector = types.Vector;

const StoredCollection = types.StoredCollection;
const IndexType = types.IndexType;

pub const Collection = struct {
    allocator: Allocator,

    db: *Db,
    stored: StoredCollection,

    index: ?VectorIndex = null,
    quantizer: ?Quantizer = null,

    pub fn init(db: *Db, stored: StoredCollection) !*Collection {
        const collec = try db.allocator.create(Collection);

        if (stored.first_child_page == 0) {
            collec.* = Collection{
                .allocator = db.allocator,
                .db = db,
                .stored = stored,
            };
            return collec;
        }
        const index = try indexes.FlatVectorIndex.init(
            db.allocator,
            db.pager,
            stored.first_child_page,
        );

        collec.* = Collection{
            .allocator = db.allocator,
            .db = db,
            .stored = stored,
            .index = index.vector_index(),
        };
        return collec;
    }

    pub fn deinit(collection: *Collection) void {
        collection.allocator.destroy(collection);
    }

    pub fn add(collection: *Collection, vector: types.Vector) !void {
        if (collection.stored.dimensions != vector.position.len) {
            return error.InvalidDimensions;
        }
        if (collection.index == null) {
            const first_child_page = try collection.db.pager.new_page(.vector, .{
                .prev_page = 0,
                .vector_size = collection.vector_size(),
            });
            defer collection.db.pager.release_page(first_child_page);

            collection.stored.first_child_page = first_child_page.header.page_num;
            try collection.db.collections.update(collection.stored);

            const index = try indexes.FlatVectorIndex.init(
                collection.allocator,
                collection.db.pager,
                first_child_page.header.page_num,
            );
            collection.index = index.vector_index();
        }

        const stored_vector = types.StoredVector{
            .id = vector.id,
            .position_data = std.mem.sliceAsBytes(vector.position),
        };
        try collection.index.?.add(stored_vector);
    }

    pub fn vector_size(collection: *Collection) u32 {
        if (collection.quantizer == null) {
            return @sizeOf(u32) + collection.stored.dimensions * @sizeOf(f32);
        }
        return 0;
    }
};
