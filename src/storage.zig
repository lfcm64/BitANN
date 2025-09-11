const std = @import("std");
const types = @import("types.zig");
const pages = @import("pages.zig");

const Allocator = std.mem.Allocator;
const Metadata = types.Metadata;

pub const Storage = struct {
    allocator: Allocator,
    file_path: []const u8,
    file: std.fs.File,
    page_size: u32,

    pub fn init(allocator: Allocator, file_path: []const u8) !Storage {
        if (file_exists(file_path)) {
            return Storage.init_existing(allocator, file_path);
        } else {
            return Storage.init_new(allocator, file_path);
        }
    }

    fn init_new(allocator: Allocator, file_path: []const u8) !Storage {
        const file = try std.fs.createFileAbsolute(file_path, .{ .read = true, .truncate = false });

        const meta = Metadata{};
        const meta_page = pages.MetadataPage{
            .header = .{ .page_num = 0, .page_type = .metadata },
            .meta = meta,
        };

        var buf: [meta.page_size]u8 = std.mem.zeroes([meta.page_size]u8);
        @memcpy(buf[0..@sizeOf(pages.MetadataPage)], std.mem.asBytes(&meta_page));
        try file.writeAll(&buf);

        return Storage{
            .allocator = allocator,
            .file_path = try allocator.dupe(u8, file_path),
            .file = file,
            .page_size = meta.page_size,
        };
    }

    fn init_existing(allocator: Allocator, file_path: []const u8) !Storage {
        const file = try std.fs.openFileAbsolute(file_path, .{ .mode = .read_write });
        errdefer file.close();

        var metadata_bytes: [@sizeOf(pages.MetadataPage)]u8 = undefined;
        const bytes_read = try file.readAll(&metadata_bytes);
        if (bytes_read != @sizeOf(pages.MetadataPage)) {
            return error.InvalidMetadata;
        }
        const meta_page: *pages.MetadataPage = @ptrCast(@alignCast(&metadata_bytes));

        return Storage{
            .allocator = allocator,
            .file_path = try allocator.dupe(u8, file_path),
            .file = file,
            .page_size = meta_page.meta.page_size,
        };
    }

    pub fn deinit(storage: *Storage) void {
        storage.file.close();
        storage.allocator.free(storage.file_path);
    }

    pub fn validate(storage: *Storage) !void {
        const meta = try storage.metadata();
        _ = meta;
    }

    pub fn metadata(storage: *Storage) !Metadata {
        var metadata_bytes: [@sizeOf(pages.MetadataPage)]u8 = undefined;
        const bytes_read = try storage.file.readAll(&metadata_bytes);
        if (bytes_read != @sizeOf(pages.MetadataPage)) {
            return error.InvalidMetadata;
        }

        const meta_page: *pages.MetadataPage = @ptrCast(@alignCast(&metadata_bytes));

        const meta_copy = meta_page.meta;
        return meta_copy;
    }

    pub fn reader(storage: *Storage) Reader {
        return Reader.new(storage.file, storage.page_size);
    }

    pub fn writer(storage: *Storage) Writer {
        return Writer.new(storage.file, storage.page_size);
    }
};

fn file_exists(file_path: []const u8) bool {
    std.fs.accessAbsolute(file_path, .{}) catch return false;
    return true;
}

pub const Reader = struct {
    file: std.fs.File,
    page_size: usize,

    pub fn new(file: std.fs.File, page_size: usize) Reader {
        return Reader{
            .file = file,
            .page_size = page_size,
        };
    }

    pub fn read_page(reader: *Reader, page_num: u32, bytes: []u8) !void {
        if (bytes.len != reader.page_size) return error.InvalidPageSize;

        const offset = page_num * reader.page_size;

        try reader.file.seekTo(offset);
        const rlen = try reader.file.readAll(bytes);

        if (rlen != reader.page_size) {
            return error.IncompleteRead;
        }
    }
};

pub const Writer = struct {
    file: std.fs.File,
    page_size: usize,

    pub fn new(file: std.fs.File, page_size: usize) Writer {
        return Writer{
            .file = file,
            .page_size = page_size,
        };
    }

    pub fn write_page(writer: *Writer, page_num: u32, rpage: []u8) !void {
        if (rpage.len != writer.page_size) return error.InvalidPageSize;

        const offset = page_num * writer.page_size;
        const required_size = (page_num + 1) * writer.page_size;

        const stat = try writer.file.stat();
        if (required_size > stat.size) {
            try writer.extend_file(page_num + 1);
        }

        try writer.file.seekTo(offset);
        try writer.file.writeAll(rpage);
    }

    fn extend_file(writer: *Writer, pcount: u32) !void {
        const nsize = pcount * writer.page_size;
        const stat = try writer.file.stat();
        if (nsize <= stat.size) return;

        try writer.file.seekTo(nsize - 1);
        try writer.file.writeAll(&[_]u8{0});
    }
};

// Helper function to create a temporary test file
fn create_test_file() !std.fs.File {
    const temp_dir = std.testing.tmpDir(.{});
    return temp_dir.dir.createFile("test.db", .{ .read = true });
}

test "write and read" {
    const testing = std.testing;

    const file = try create_test_file();
    defer file.close();

    var writer = Writer.new(file, 1024);
    var reader = Reader.new(file, 1024);

    var page = [_]u8{0x42} ** 1024;
    try writer.write_page(0, &page);

    var buf = [_]u8{0} ** 1024;
    try reader.read_page(0, &buf);

    try testing.expectEqualSlices(u8, &page, &buf);
}

test "auto file extension" {
    const testing = std.testing;

    const file = try create_test_file();
    defer file.close();

    var writer = Writer.new(file, 100);
    var page = [_]u8{0xAA} ** 100;

    try writer.write_page(5, &page);

    const stat = try file.stat();
    try testing.expect(stat.size >= 600); // 6 pages minimum
}

test "invalid page size errors" {
    const testing = std.testing;

    const file = try create_test_file();
    defer file.close();

    var writer = Writer.new(file, 1024);
    var reader = Reader.new(file, 1024);

    var wrong_buf = [_]u8{0} ** 512;

    try testing.expectError(error.InvalidPageSize, writer.write_page(0, &wrong_buf));
    try testing.expectError(error.InvalidPageSize, reader.read_page(0, &wrong_buf));
}
