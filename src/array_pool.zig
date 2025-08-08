const std = @import("std");
const assert = std.debug.assert;

pub fn FixedSizeArrayPool(comptime T: type, fixed_size: usize) type {
    return struct {
        const Pool = @This();
        const Node = struct {
            next: ?*@This(),
        };

        arena: std.heap.ArenaAllocator,
        free_list: ?*Node = null,

        pub fn init(allocator: std.mem.Allocator) !Pool {
            return .{ .arena = std.heap.ArenaAllocator.init(allocator) };
        }

        pub fn deinit(pool: *Pool) void {
            pool.arena.deinit();
            pool.* = undefined;
        }

        pub fn acquire(pool: *Pool) ![]T {
            if (pool.free_list) |node| {
                pool.free_list = node.next;
                const arr = @as([*]T, @ptrCast(@alignCast(node)));
                return arr[0..fixed_size];
            }
            return pool.allocNew();
        }

        pub fn release(pool: *Pool, arr: []T) void {
            const node: *Node = @ptrCast(@alignCast(arr.ptr));
            node.next = pool.free_list;
            pool.free_list = node;
        }

        fn allocNew(pool: *Pool) ![]T {
            const total_size = @max(@sizeOf(Node), @sizeOf(T) * fixed_size);
            const alignment = @max(@alignOf(Node), @alignOf(T));

            const bytes = try pool.arena.allocator().alignedAlloc(u8, alignment, total_size);
            const arr = @as([*]T, @ptrCast(@alignCast(bytes)));
            return arr[0..fixed_size];
        }
    };
}

test "Pool initialization and cleanup" {
    const testing = std.testing;

    var pool = try FixedSizeArrayPool(u32, 10).init(testing.allocator);
    defer pool.deinit();

    try testing.expect(pool.free_list == null);
}

test "Array release and reuse" {
    const testing = std.testing;
    var pool = try FixedSizeArrayPool(u32, 3).init(testing.allocator);
    defer pool.deinit();

    const arr1 = try pool.acquire();
    try testing.expect(arr1.len == 3);

    arr1[0] = 1;
    arr1[1] = 2;
    arr1[2] = 3;

    pool.release(arr1);
    try testing.expect(pool.free_list != null);

    const arr2 = try pool.acquire();
    try testing.expect(arr2.len == 3);
    try testing.expect(pool.free_list == null);

    // first 2 values (2 * 4bytes) should be 0 because Node.next was set to null
    try testing.expect(arr2[0] == 0);
    try testing.expect(arr2[1] == 0);
    //last element should stay the same
    try testing.expect(arr2[2] == 3);

    pool.release(arr2);
}

test "Multiple arrays without release" {
    const testing = std.testing;

    var pool = try FixedSizeArrayPool(f32, 4).init(testing.allocator);
    defer pool.deinit();

    const arr1 = try pool.acquire();
    const arr2 = try pool.acquire();
    const arr3 = try pool.acquire();

    try testing.expect(arr1.len == 4);
    try testing.expect(arr2.len == 4);
    try testing.expect(arr3.len == 4);

    arr1[0] = 1.1;
    arr2[0] = 2.2;
    arr3[0] = 3.3;

    try testing.expect(arr1[0] == 1.1);
    try testing.expect(arr2[0] == 2.2);
    try testing.expect(arr3[0] == 3.3);
}
