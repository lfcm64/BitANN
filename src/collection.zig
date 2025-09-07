const std = @import("std");
const types = @import("types.zig");
const manager = @import("manager.zig");
const Pager = @import("pager.zig").Pager;
const Db = @import("db.zig");

const Allocator = std.mem.Allocator;
const Vector = types.Vector;
const ItemManager = manager.ItemManager;

pub const Collection = struct {
    allocator: Allocator,
    collection: types.Collection,
    manager: ItemManager(.vector),
    pager: *Pager,

    pub fn init(allocator: Allocator, pager: *Pager, collection: types.Collection) !*Collection {
        const self = try allocator.create(Collection);
        self.* = Collection{
            .allocator = allocator,
            .collection = collection,
            .manager = try ItemManager(.vector).init(pager, collection.index.flat.first_vector_page),
            .pager = pager,
        };
        return self;
    }

    pub fn deinit(self: *Collection) void {
        self.allocator.destroy(self);
    }

    pub fn add(handle: *Collection, vector: Vector) !void {
        switch (handle.collection.index) {
            .flat => {
                try handle.manager.append(vector);
            },
        }
    }
};
