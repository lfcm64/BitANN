const std = @import("std");
const pages = @import("pages.zig");

const testing = std.testing;

const RawPage = pages.RawPage;

const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

pub const PagePool = struct {
    const Node = struct {
        next: ?*@This(),
    };

    allocator: Allocator,
    arena: ArenaAllocator,

    page_size: usize,

    free_list: ?*Node = null,

    pub fn init(allocator: Allocator, page_size: usize) !*PagePool {
        const pool = try allocator.create(PagePool);

        pool.* = .{
            .allocator = allocator,
            .arena = std.heap.ArenaAllocator.init(allocator),
            .page_size = page_size,
        };
        return pool;
    }

    pub fn initPreheated(allocator: Allocator, size: usize, page_size: usize) !*PagePool {
        var pool = try PagePool.init(allocator, page_size);
        try pool.preheat(size);
        return pool;
    }

    pub fn deinit(pool: *PagePool) void {
        pool.arena.deinit();
        pool.allocator.destroy(pool);
    }

    pub fn acquire(pool: *PagePool) !RawPage {
        if (pool.free_list) |node| {
            pool.free_list = node.next;
            const arr = @as([*]u8, @ptrCast(@alignCast(node)));
            return arr[0..pool.page_size];
        }
        return pool.allocNew();
    }

    pub fn release(pool: *PagePool, raw_page: RawPage) void {
        const node: *Node = @ptrCast(@alignCast(raw_page.ptr));
        node.next = pool.free_list;
        pool.free_list = node;
    }

    pub fn preheat(pool: *PagePool, size: usize) !void {
        var i: usize = 0;
        while (i < size) : (i += 1) {
            const rpage = try pool.allocNew();
            pool.release(rpage);
        }
    }

    fn allocNew(pool: *PagePool) !RawPage {
        const total_size = @max(@sizeOf(Node), @sizeOf(u8) * pool.page_size);
        const alignment = @max(@alignOf(Node), @alignOf(u8));

        const bytes = try pool.arena.allocator().alignedAlloc(u8, alignment, total_size);
        const arr = @as([*]u8, @ptrCast(@alignCast(bytes)));
        return arr[0..pool.page_size];
    }
};
test "acquire/release" {
    var pool = try PagePool.init(testing.allocator, 256);
    defer pool.deinit();

    const page = try pool.acquire();
    try testing.expectEqual(page.len, 256);

    pool.release(page);
    const reused = try pool.acquire();
    try testing.expectEqual(reused.ptr, page.ptr);
}

test "multiple release/acquire" {
    var pool = try PagePool.init(testing.allocator, 128);
    defer pool.deinit();

    const page1 = try pool.acquire();
    const page2 = try pool.acquire();

    const ptr1 = page1.ptr;
    const ptr2 = page2.ptr;

    pool.release(page1);
    pool.release(page2);

    const reused1 = try pool.acquire();
    const reused2 = try pool.acquire();

    try testing.expectEqual(reused1.ptr, ptr2);
    try testing.expectEqual(reused2.ptr, ptr1);
}

test "preheat" {
    var pool = try PagePool.init(testing.allocator, 128);
    defer pool.deinit();

    try pool.preheat(3);

    const p1 = try pool.acquire();
    const p2 = try pool.acquire();
    const p3 = try pool.acquire();

    pool.release(p1);
    pool.release(p2);
    pool.release(p3);
}

test "initPreheated" {
    var pool = try PagePool.initPreheated(testing.allocator, 10, 200);
    defer pool.deinit();

    const p1 = try pool.acquire();
    const p2 = try pool.acquire();

    pool.release(p1);
    pool.release(p2);
}
