const std = @import("std");
const Db = @import("db.zig");

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const cwd = try std.process.getCwdAlloc(allocator);
    defer allocator.free(cwd);

    const abs_path = try std.fs.path.join(allocator, &[_][]const u8{ cwd, "test.db" });
    defer allocator.free(abs_path);

    const db = try Db.init(allocator, abs_path);
    defer db.deinit();

    try db.add_index(0, 4);

    var point = [4]f32{ 0.0, 0.0, 0.0, 0.0 };

    const start = std.time.milliTimestamp();

    for (0..10000) |_| {
        try db.add_vector(0, .{ .id = 3, .point = &point });
    }
    std.debug.print(("elapsed time: {}\n"), .{std.time.milliTimestamp() - start});

    try db.flush();
}
