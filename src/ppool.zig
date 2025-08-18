const std = @import("std");

const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const Node = struct {
    next: ?*@This(),
};

pub const PPool = struct {
    allocator: Allocator,
    arena: ArenaAllocator,

    psize: usize,

    free_list: ?*Node = null,

    pub fn init(allocator: Allocator, psize: usize) !*PPool {
        const pool = try allocator.create(PPool);

        pool.* = .{
            .allocator = allocator,
            .arena = std.heap.ArenaAllocator.init(allocator),
            .psize = psize,
        };
        return pool;
    }

    pub fn initPreheated(allocator: Allocator, size: usize, psize: usize) !*PPool {
        var pool = try PPool.init(allocator, psize);
        try pool.preheat(size);
        return pool;
    }

    pub fn deinit(pool: *PPool) void {
        pool.arena.deinit();
        pool.allocator.destroy(pool);
    }

    pub fn acquire(pool: *PPool) ![]u8 {
        if (pool.free_list) |node| {
            pool.free_list = node.next;
            const arr = @as([*]u8, @ptrCast(@alignCast(node)));
            return arr[0..pool.psize];
        }
        return pool.allocNew();
    }

    pub fn release(pool: *PPool, rpage: []u8) void {
        const node: *Node = @ptrCast(@alignCast(rpage.ptr));
        node.next = pool.free_list;
        pool.free_list = node;
    }

    pub fn preheat(pool: *PPool, size: usize) !void {
        var i: usize = 0;
        while (i < size) : (i += 1) {
            const rpage = try pool.allocNew();
            pool.release(rpage);
        }
    }

    fn allocNew(pool: *PPool) ![]u8 {
        const total_size = @max(@sizeOf(Node), @sizeOf(u8) * pool.psize);
        const alignment = @max(@alignOf(Node), @alignOf(u8));

        const bytes = try pool.arena.allocator().alignedAlloc(u8, alignment, total_size);
        const arr = @as([*]u8, @ptrCast(@alignCast(bytes)));
        return arr[0..pool.psize];
    }
};

test "acquire/release" {
    const testing = std.testing;

    var pool = try PPool.init(testing.allocator, 256);
    defer pool.deinit();

    const page = try pool.acquire();
    try testing.expect(page.len == 256);

    pool.release(page);
    const reused = try pool.acquire();
    try testing.expect(reused.ptr == page.ptr);
}

test "multiple release/acquire" {
    const testing = std.testing;
    var pool = try PPool.init(testing.allocator, 128);
    defer pool.deinit();

    const page1 = try pool.acquire();
    const page2 = try pool.acquire();

    const ptr1 = page1.ptr;
    const ptr2 = page2.ptr;

    pool.release(page1);
    pool.release(page2);

    const reused1 = try pool.acquire();
    const reused2 = try pool.acquire();

    try testing.expect(reused1.ptr == ptr2);
    try testing.expect(reused2.ptr == ptr1);
}

test "preheat" {
    const testing = std.testing;

    var pool = try PPool.init(testing.allocator, 128);
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
    const testing = std.testing;

    var pool = try PPool.initPreheated(testing.allocator, 10, 200);
    defer pool.deinit();

    const p1 = try pool.acquire();
    const p2 = try pool.acquire();

    pool.release(p1);
    pool.release(p2);
}
