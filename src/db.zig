const std = @import("std");
const types = @import("types.zig");
const pages = @import("pages.zig");

const Pager = @import("pager.zig");
const Index = @import("index.zig");

const fs = std.fs;

const Metadata = types.Metadata;
const Allocator = std.mem.Allocator;

const DbStatus = enum {
    uninitialized,
    initialized,
};

const Db = @This();

allocator: Allocator,
meta: Metadata,
pager: *Pager,
file: std.fs.File,
index_map: std.AutoArrayHashMapUnmanaged(u32, Index) = .{},

pub fn init(allocator: Allocator, file_path: []const u8) !*Db {
    const status = get_status(file_path);

    const file = switch (status) {
        .uninitialized => try fs.createFileAbsolute(file_path, .{ .read = true, .truncate = false }),
        .initialized => try fs.openFileAbsolute(file_path, .{ .mode = .read_write }),
    };
    errdefer file.close();

    const meta = switch (status) {
        .uninitialized => Metadata{},
        .initialized => try read_metadata(file),
    };
    const mode: Pager.PagerInitMode = switch (status) {
        .uninitialized => .create_new,
        .initialized => .open_existing,
    };
    const pager = try Pager.init(allocator, file, meta, mode);
    errdefer pager.deinit();

    const db = try allocator.create(Db);
    db.* = Db{
        .allocator = allocator,
        .meta = meta,
        .pager = pager,
        .file = file,
    };
    return db;
}

pub fn deinit(db: *Db) void {
    db.index_map.deinit(db.allocator);
    db.pager.deinit();
    db.file.close();
    db.allocator.destroy(db);
}

pub fn add_index(db: *Db, id: u32, vector_dim: u32) !void {
    const index = try Index.init(db.allocator, db.pager, db.meta.page_size, id, vector_dim);
    try db.index_map.put(db.allocator, id, index);
}

pub fn add_vector(db: *Db, index_id: u32, vector: types.Vector) !void {
    const index = db.index_map.getPtr(index_id) orelse return error.IndexNotFound;
    try index.add_vector(vector);
}

pub fn flush(db: *Db) !void {
    try db.pager.flush_cache();
}

fn get_status(file_path: []const u8) DbStatus {
    std.fs.accessAbsolute(file_path, .{}) catch return .uninitialized;
    return .initialized;
}

fn read_metadata(file: std.fs.File) !Metadata {
    const metadata_offset = @sizeOf(pages.PageHeader);
    var buf: [@sizeOf(Metadata)]u8 = undefined;

    try file.seekTo(metadata_offset);
    if (try file.readAll(&buf) != @sizeOf(Metadata)) {
        return error.IncompleteMetadata;
    }

    const metadata_ptr: *Metadata = @ptrCast(@alignCast(&buf));
    return metadata_ptr.*;
}
