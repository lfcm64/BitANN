const std = @import("std");
const storage = @import("storage.zig");
const indexes = @import("indexes.zig");

const Pager = @import("pager.zig").Pager;
const Collection = @import("collection.zig").Collection;

const fs = std.fs;
const Allocator = std.mem.Allocator;
const Storage = storage.Storage;
const CollectionIndex = indexes.CollectionIndex;

pub const Db = struct {
    allocator: Allocator,
    storage: Storage,
    pager: *Pager,
    collections: CollectionIndex,

    pub fn open(allocator: Allocator, file_path: []const u8) !*Db {
        var sto = try Storage.init(allocator, file_path);
        errdefer sto.deinit();

        //try file.validate();

        const pager = try Pager.init(allocator, &sto);
        errdefer pager.deinit();

        const db = try allocator.create(Db);
        db.* = Db{
            .allocator = allocator,
            .storage = sto,
            .pager = pager,
            .collections = try CollectionIndex.init(allocator, pager),
        };
        return db;
    }

    pub fn close(db: *Db) void {
        db.collections.deinit();
        db.pager.flush_cache(.hard) catch {};
        db.pager.deinit();
        db.storage.deinit();
        db.allocator.destroy(db);
    }

    pub fn create_collection(db: *Db, id: u32, dimensions: u32) !*Collection {
        try db.collections.add(id, dimensions);
        return db.collection(id);
    }

    pub fn collection(db: *Db, id: u32) !*Collection {
        const collec = try db.collections.get(id);
        return Collection.init(db, collec);
    }

    pub fn flush(db: *Db) !void {
        try db.pager.flush_cache(.hard);
    }
};
