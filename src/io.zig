const std = @import("std");

const File = std.fs.File;

pub const PageReader = struct {
    file: File,
    page_size: usize,

    pub fn new(file: File, page_size: usize) PageReader {
        return PageReader{
            .file = file,
            .page_size = page_size,
        };
    }

    pub fn read_page(reader: *PageReader, pnum: u32, bytes: []u8) !void {
        if (bytes.len != reader.page_size) return error.InvalidPageSize;

        const offset = pnum * reader.page_size;

        try reader.file.seekTo(offset);
        const rlen = try reader.file.readAll(bytes);

        if (rlen != reader.page_size) {
            return error.IncompleteRead;
        }
    }
};

pub const PageWriter = struct {
    file: File,
    page_size: usize,

    pub fn new(file: File, page_size: usize) PageWriter {
        return PageWriter{
            .file = file,
            .page_size = page_size,
        };
    }

    pub fn write_page(writer: *PageWriter, pnum: u32, rpage: []u8) !void {
        if (rpage.len != writer.page_size) return error.InvalidPageSize;

        const offset = pnum * writer.page_size;
        const required_size = (pnum + 1) * writer.page_size;

        const stat = try writer.file.stat();
        if (required_size > stat.size) {
            try writer.extend_file(pnum + 1);
        }

        try writer.file.seekTo(offset);
        try writer.file.writeAll(rpage);
    }

    fn extend_file(writer: *PageWriter, pcount: u32) !void {
        const nsize = pcount * writer.page_size;
        const stat = try writer.file.stat();
        if (nsize <= stat.size) return;

        try writer.file.seekTo(nsize - 1);
        try writer.file.writeAll(&[_]u8{0});
    }
};

// Helper function to create a temporary test file
fn create_test_file() !File {
    const temp_dir = std.testing.tmpDir(.{});
    return temp_dir.dir.createFile("test.db", .{ .read = true });
}

test "write and read" {
    const testing = std.testing;

    const file = try create_test_file();
    defer file.close();

    var writer = PageWriter.new(file, 1024);
    var reader = PageReader.new(file, 1024);

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

    var writer = PageWriter.new(file, 100);
    var page = [_]u8{0xAA} ** 100;

    try writer.write_page(5, &page);

    const stat = try file.stat();
    try testing.expect(stat.size >= 600); // 6 pages minimum
}

test "invalid page size errors" {
    const testing = std.testing;

    const file = try create_test_file();
    defer file.close();

    var writer = PageWriter.new(file, 1024);
    var reader = PageReader.new(file, 1024);

    var wrong_buf = [_]u8{0} ** 512;

    try testing.expectError(error.InvalidPageSize, writer.write_page(0, &wrong_buf));
    try testing.expectError(error.InvalidPageSize, reader.read_page(0, &wrong_buf));
}
