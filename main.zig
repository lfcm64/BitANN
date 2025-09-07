const std = @import("std");
const Db = @import("src/db.zig").Db;

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const cwd = try std.process.getCwdAlloc(allocator);
    defer allocator.free(cwd);

    const abs_path = try std.fs.path.join(allocator, &[_][]const u8{ cwd, "test.db" });
    defer allocator.free(abs_path);

    const db = try Db.open(allocator, abs_path);
    defer db.close();

    const collec = try db.create_collection(1, 3);
    defer collec.deinit();

    var position = [_]u8{ 0x00, 0x00, 0x80, 0x3F, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40 }; // 1.0, 2.0, 3.0

    const start = std.time.milliTimestamp();

    for (0..1_000) |_| {
        try collec.add(.{ .id = 3, .quantization = .{ .none = .{ .dimension = 3 } }, .position = &position });
    }
    try db.flush();

    std.debug.print(("elapsed time: {}\n"), .{std.time.milliTimestamp() - start});
}
