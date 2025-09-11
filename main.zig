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

    const collection = try db.create_collection(1, 3);
    defer collection.deinit();

    var position = [_]f32{ 1.0, 2.0, 3.0 };

    const start = std.time.milliTimestamp();

    for (0..1_000) |_| {
        try collection.add(.{ .id = 3, .position = &position });
    }

    std.debug.print(("elapsed time: {}\n"), .{std.time.milliTimestamp() - start});
}
