const std = @import("std");
const types = @import("types.zig");
const pages = @import("pages.zig");
const cursors = @import("cursors.zig");

const Pager = @import("pager.zig").Pager;

const Allocator = std.mem.Allocator;
const StoredVector = types.StoredVector;
const StoredCollection = types.StoredCollection;

pub const CollectionIndex = struct {
    const Cursor = cursors.ChainedItemCursor(.collection);

    allocator: Allocator,
    pager: *Pager,
    cursor: Cursor,

    pub fn init(allocator: Allocator, pager: *Pager) !CollectionIndex {
        return CollectionIndex{
            .allocator = allocator,
            .pager = pager,
            .cursor = try Cursor.init(pager, 1),
        };
    }

    pub fn deinit(index: *CollectionIndex) void {
        index.cursor.deinit();
    }

    pub fn add(index: *CollectionIndex, id: u32, dimensions: u32) !void {
        if (try index.exists(id)) {
            return error.CollectionAlreadyExists;
        }

        const collection = StoredCollection{
            .id = id,
            .dimensions = dimensions,
            .quant = .none,
            .index = .flat,
            .first_child_page = 0,
        };

        index.cursor.next_empty_slot() catch {
            try index.cursor.seek_to_end();
            const old_last_page = index.cursor.page;

            const new_last_page = try index.pager.new_page(
                .collection,
                .{ .prev_page = old_last_page.header.page_num },
            );
            old_last_page.next_page = new_last_page.header.page_num;
            try index.cursor.next_empty_slot();
        };
        try index.cursor.page.insert(index.cursor.index, collection);
    }

    pub fn get(index: *CollectionIndex, id: u32) !StoredCollection {
        var it = index.cursor.iterator();
        while (try it.next()) |collec| {
            if (collec.id == id) return collec;
        }
        return error.CollectionNotFound;
    }

    pub fn update(index: *CollectionIndex, updated: StoredCollection) !void {
        var it = index.cursor.iterator();
        while (try it.next()) |collec| {
            if (collec.id == updated.id) {
                try it.cursor.page.insert(it.cursor.index, updated);
                return;
            }
        }
        return error.CollectionNotFound;
    }

    pub fn exists(index: *CollectionIndex, id: u32) !bool {
        var it = index.cursor.iterator();
        while (try it.next()) |collec| {
            if (collec.id == id) return true;
        }
        return false;
    }
};

pub const VectorIndex = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        deinit: *const fn (ptr: *anyopaque) void,
        add: *const fn (ptr: *anyopaque, vector: StoredVector) anyerror!void,
        //search: *const fn () anyerror!void,
    };

    pub fn deinit(self: VectorIndex) void {
        self.vtable.deinit(self.ptr);
    }

    pub fn add(self: VectorIndex, vector: StoredVector) !void {
        try self.vtable.add(self.ptr, vector);
    }
};

pub const FlatVectorIndex = struct {
    const Cursor = cursors.ChainedItemCursor(.vector);

    allocator: Allocator,
    first_vector_page: u32,
    pager: *Pager,
    cursor: Cursor,

    pub fn init(allocator: Allocator, pager: *Pager, first_vector_page: u32) !*FlatVectorIndex {
        const index = try allocator.create(FlatVectorIndex);
        index.* = FlatVectorIndex{
            .allocator = allocator,
            .first_vector_page = first_vector_page,
            .pager = pager,
            .cursor = try Cursor.init(pager, first_vector_page),
        };
        return index;
    }

    pub fn deinit(index: *FlatVectorIndex) void {
        index.cursor.deinit();
        index.allocator.destroy(index);
    }

    pub fn deinit_erased(ptr: *anyopaque) void {
        const index: *FlatVectorIndex = @ptrCast(@alignCast(ptr));
        index.deinit();
    }

    pub fn vector_index(index: *FlatVectorIndex) VectorIndex {
        return VectorIndex{
            .ptr = index,
            .vtable = &.{
                .deinit = deinit_erased,
                .add = add_erased,
            },
        };
    }

    pub fn add(index: *FlatVectorIndex, vector: StoredVector) !void {
        index.cursor.next_empty_slot() catch {
            try index.cursor.seek_to_end();
            const old_last_page = index.cursor.page;

            const new_last_page = try index.pager.new_page(.vector, .{
                .prev_page = old_last_page.header.page_num,
                .vector_size = old_last_page.item_size,
            });
            old_last_page.next_page = new_last_page.header.page_num;
            try index.cursor.next_empty_slot();
        };
        try index.cursor.page.insert(index.cursor.index, vector);
    }

    fn add_erased(ptr: *anyopaque, vector: StoredVector) anyerror!void {
        const index: *FlatVectorIndex = @ptrCast(@alignCast(ptr));
        return index.add(vector);
    }
};

pub const IVFVectorIndex = struct {};
