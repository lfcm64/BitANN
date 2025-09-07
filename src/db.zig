const std = @import("std");
const types = @import("types.zig");
const pages = @import("pages.zig");
const cursors = @import("cursors.zig");
const pagers = @import("pager.zig");
const storage = @import("storage.zig");
const manager = @import("manager.zig");
const Collection = @import("collection.zig").Collection;

const Pager = pagers.Pager;

const fs = std.fs;

const Metadata = types.Metadata;
const Allocator = std.mem.Allocator;
const ItemCursor = cursors.ItemCursor;
const Storage = storage.Storage;

pub const Db = struct {
    allocator: Allocator,
    pager: *Pager,
    storage: Storage,

    pub fn open(allocator: Allocator, file_path: []const u8) !*Db {
        var sto = try Storage.init(allocator, file_path);
        errdefer sto.deinit();

        //try file.validate();

        const pager = try Pager.init(allocator, &sto);
        errdefer pager.deinit();

        const db = try allocator.create(Db);
        db.* = Db{
            .allocator = allocator,
            .pager = pager,
            .storage = sto,
        };
        return db;
    }

    pub fn close(db: *Db) void {
        db.pager.deinit();
        db.storage.deinit();
        db.allocator.destroy(db);
    }

    pub fn create_collection(db: *Db, id: u32, dimensions: u32) !*Collection {
        var cursor = try ItemCursor(.collection).init(db.pager, 1);
        defer cursor.deinit();

        var it = cursor.iterator();
        while (try it.next()) |collec| {
            if (collec.id == id) return error.CollectionAlreadyExist;
        }

        const quantization = types.Quantization{ .none = .{ .dimension = dimensions } };
        const vec_page = try db.pager.new_page(.vector, .{ .prev_page = 0, .quantization = quantization });
        const collec = types.Collection{
            .id = id,
            .index = .{ .flat = .{ .first_vector_page = vec_page.header.page_num } },
            .quantization = quantization,
            .vector_count = 0,
        };
        var man = try manager.ItemManager(.collection).init(db.pager, 1);
        defer man.deinit();

        try man.append(collec);
        return Collection.init(db.allocator, db.pager, collec);
    }

    pub fn collection(db: *Db, id: u32) !*Collection {
        var cursor = try ItemCursor(.collection).init(db.pager, 1);
        defer cursor.deinit();

        var it = cursor.iterator();
        while (try it.next()) |collec| {
            if (collec.id == id) return Collection.init(db.allocator, db.pager, collec);
        }
        return error.CollectionNotFound;
    }

    pub fn flush(db: *Db) !void {
        try db.pager.flush_cache(.hard);
    }
};
