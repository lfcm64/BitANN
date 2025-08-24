const std = @import("std");

const File = std.fs.File;

pub const PReader = struct {
    file: File, // database file
    psize: usize, // page size

    pub fn new(file: File, psize: usize) PReader {
        return PReader{
            .file = file,
            .psize = psize,
        };
    }

    pub fn readPage(reader: *PReader, pnum: u32, bytes: []u8) !void {
        if (bytes.len != reader.psize) return error.InvalidPageSize;

        const offset = pnum * reader.psize;

        try reader.file.seekTo(offset);
        const rlen = try reader.file.readAll(bytes);

        if (rlen != reader.psize) {
            return error.IncompleteRead;
        }
    }
};

pub const PWriter = struct {
    file: File, // database file
    psize: usize, // page size

    pub fn new(file: File, psize: usize) PWriter {
        return PWriter{
            .file = file,
            .psize = psize,
        };
    }

    pub fn writePage(writer: *PWriter, pnum: u32, rpage: []u8) !void {
        if (rpage.len != writer.psize) return error.InvalidPageSize;

        const offset = pnum * writer.psize;
        const required_size = (pnum + 1) * writer.psize;

        const stat = try writer.file.stat();
        if (required_size > stat.size) {
            try writer.extendFile(pnum + 1);
        }

        try writer.file.seekTo(offset);
        try writer.file.writeAll(rpage);
    }

    fn extendFile(writer: *PWriter, pcount: u32) !void {
        const nsize = pcount * writer.psize;
        const stat = try writer.file.stat();
        if (nsize <= stat.size) return;

        try writer.file.seekTo(nsize - 1);
        try writer.file.writeAll(&[_]u8{0});
    }
};

// Helper function to create a temporary test file
fn createTestFile() !File {
    const temp_dir = std.testing.tmpDir(.{});
    return temp_dir.dir.createFile("test.db", .{ .read = true });
}

test "write and read" {
    const testing = std.testing;

    const file = try createTestFile();
    defer file.close();

    var writer = PWriter.new(file, 1024);
    var reader = PReader.new(file, 1024);

    var rpage = [_]u8{0x42} ** 1024;
    try writer.writePage(0, &rpage);

    var buf = [_]u8{0} ** 1024;
    try reader.readPage(0, &buf);

    try testing.expectEqualSlices(u8, &rpage, &buf);
}

test "auto file extension" {
    const testing = std.testing;

    const file = try createTestFile();
    defer file.close();

    var writer = PWriter.new(file, 100);
    var rpage = [_]u8{0xAA} ** 100;

    try writer.writePage(5, &rpage);

    const stat = try file.stat();
    try testing.expect(stat.size >= 600); // 6 pages minimum
}

test "invalid page size errors" {
    const testing = std.testing;

    const file = try createTestFile();
    defer file.close();

    var writer = PWriter.new(file, 1024);
    var reader = PReader.new(file, 1024);

    var wrong_buf = [_]u8{0} ** 512;

    try testing.expectError(error.InvalidPageSize, writer.writePage(0, &wrong_buf));
    try testing.expectError(error.InvalidPageSize, reader.readPage(0, &wrong_buf));
}
