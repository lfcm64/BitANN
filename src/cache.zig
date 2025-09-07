const std = @import("std");

const testing = std.testing;
const Allocator = std.mem.Allocator;

pub const PageHandleState = enum {
    clean,
    dirty,
};

pub const PageHandle = struct {
    refs: u32,
    state: PageHandleState,

    page_num: u32,
    raw: []u8,
};

/// Called every time a cached page is evicted from the cache
pub const EvictionCallBack = struct {
    ptr: *anyopaque,
    on_evict_fn: *const fn (*anyopaque, *PageHandle) anyerror!void,

    pub fn on_evict(cb: *EvictionCallBack, handle: *PageHandle) !void {
        try cb.on_evict_fn(cb.ptr, handle);
    }
};

pub const PageCache = struct {
    const DoublyLinkedList = std.DoublyLinkedList;
    const AutoHashMapUnmanaged = std.AutoHashMapUnmanaged;

    const Node = DoublyLinkedList(PageHandle).Node;

    allocator: Allocator,

    capacity: usize,

    table: AutoHashMapUnmanaged(u32, *Node) = .{},
    lru: DoublyLinkedList(PageHandle) = .{},

    eviction: EvictionCallBack,

    pub fn init(allocator: Allocator, capacity: usize, eviction: EvictionCallBack) !*PageCache {
        const cache = try allocator.create(PageCache);
        cache.* = .{
            .allocator = allocator,
            .capacity = capacity,
            .eviction = eviction,
        };
        return cache;
    }

    pub fn deinit(cache: *PageCache) void {
        var it = cache.table.valueIterator();
        while (it.next()) |node_ptr| {
            cache.allocator.destroy(node_ptr.*);
        }
        cache.table.deinit(cache.allocator);
        cache.allocator.destroy(cache);
    }

    pub fn get(cache: *PageCache, page_num: u32) ?*PageHandle {
        if (cache.table.get(page_num)) |node| {
            if (node.data.refs == 0) {
                cache.lru.remove(node);
            }
            node.data.refs += 1;
            return &node.data;
        }
        return null;
    }

    pub fn release(cache: *PageCache, page_num: u32) void {
        if (cache.table.get(page_num)) |node| {
            if (node.data.refs == 1) {
                cache.lru.prepend(node);
            }
            node.data.refs -= 1;
        }
    }

    pub fn put(cache: *PageCache, handle: PageHandle) !void {
        if (cache.table.contains(handle.page_num)) return;

        if (cache.count() >= cache.capacity) {
            try cache.evict_last();
        }

        const node = try cache.allocator.create(Node);
        node.* = .{ .data = handle };

        if (node.data.refs == 0) {
            cache.lru.prepend(node);
        }
        try cache.table.put(cache.allocator, handle.page_num, node);
    }

    pub fn count(cache: *PageCache) usize {
        return cache.table.count();
    }

    pub fn contains(cache: *PageCache, page_num: u32) bool {
        return cache.table.contains(page_num);
    }

    pub fn contains_in_lru(cache: *PageCache, page_num: u32) bool {
        const node = cache.table.get(page_num) orelse return false;
        return node.data.refs == 0;
    }

    pub fn flush(cache: *PageCache) !void {
        while (cache.lru.first != null) {
            try cache.evict_last();
        }
    }

    pub fn flush_hard(cache: *PageCache) !void {
        while (cache.lru.first != null) {
            try cache.evict_last();
        }
        var it = cache.table.valueIterator();
        while (it.next()) |node| {
            try cache.eviction.on_evict(&node.*.data);
            _ = cache.table.remove(node.*.data.page_num);
        }
    }

    fn evict_last(cache: *PageCache) !void {
        if (cache.lru.last) |node| {
            try cache.eviction.on_evict(&node.data);

            _ = cache.table.remove(node.data.page_num);
            cache.lru.remove(node);
            cache.allocator.destroy(node);
        }
    }
};

// Test helper: Simple eviction counter
const TestEviction = struct {
    counter: u32 = 0,

    fn on_evict(ptr: *anyopaque, handle: *PageHandle) anyerror!void {
        _ = handle;
        const self: *TestEviction = @ptrCast(@alignCast(ptr));
        self.counter += 1;
    }

    fn eviction(self: *TestEviction) EvictionCallBack {
        return EvictionCallBack{ .ptr = self, .on_evict_fn = on_evict };
    }
};

test "cache put and get" {
    var test_eviction = TestEviction{};
    const eviction = test_eviction.eviction();

    var cache = try PageCache.init(testing.allocator, 2, eviction);
    defer cache.deinit();

    var raw_page = [_]u8{0x42} ** 256;
    const handle = PageHandle{ .refs = 0, .page_num = 1, .raw = &raw_page, .state = .clean };
    try cache.put(handle);

    const retrieved = cache.get(1);
    try testing.expectEqual(retrieved.?.page_num, 1);
    try testing.expectEqual(retrieved.?.state, .clean);
    try testing.expectEqual(retrieved.?.refs, 1);

    try testing.expect(!cache.contains_in_lru(1));

    cache.release(1);
    try testing.expect(cache.contains_in_lru(1));
    try testing.expectEqual(cache.table.get(1).?.data.refs, 0);
}

test "cache eviction when full" {
    var test_eviction = TestEviction{};
    const eviction = test_eviction.eviction();

    var cache = try PageCache.init(testing.allocator, 2, eviction);
    defer cache.deinit();

    var raw_page1 = [_]u8{0x11} ** 128;
    var raw_page2 = [_]u8{0x22} ** 128;
    var raw_page3 = [_]u8{0x33} ** 128;

    const handle1 = PageHandle{ .refs = 0, .page_num = 1, .raw = &raw_page1, .state = .clean };
    const handle2 = PageHandle{ .refs = 0, .page_num = 2, .raw = &raw_page2, .state = .clean };
    const handle3 = PageHandle{ .refs = 0, .page_num = 3, .raw = &raw_page3, .state = .clean };

    try cache.put(handle1);
    try cache.put(handle2);

    try testing.expectEqual(cache.count(), 2);

    try cache.put(handle3);

    try testing.expectEqual(test_eviction.counter, 1);
    try testing.expectEqual(cache.count(), 2);

    try testing.expect(!cache.contains(1));
    try testing.expect(cache.contains(2));
    try testing.expect(cache.contains(3));
}

test "cache flush" {
    var test_eviction = TestEviction{};
    const eviction = test_eviction.eviction();

    var cache = try PageCache.init(testing.allocator, 5, eviction);
    defer cache.deinit();

    var raw_page1 = [_]u8{0x11} ** 128;
    var raw_page2 = [_]u8{0x22} ** 128;
    var raw_page3 = [_]u8{0x33} ** 128;

    const handle1 = PageHandle{ .refs = 1, .page_num = 1, .raw = &raw_page1, .state = .clean };
    const handle2 = PageHandle{ .refs = 0, .page_num = 2, .raw = &raw_page2, .state = .clean };
    const handle3 = PageHandle{ .refs = 0, .page_num = 3, .raw = &raw_page3, .state = .clean };

    try cache.put(handle1);
    try cache.put(handle2);
    try cache.put(handle3);

    try testing.expectEqual(cache.count(), 3);

    try cache.flush();

    try testing.expectEqual(test_eviction.counter, 2);
    try testing.expectEqual(cache.count(), 1);
    try testing.expect(cache.lru.first == null);
}

test "reference counting behavior" {
    var test_eviction = TestEviction{};
    const eviction = test_eviction.eviction();

    var cache = try PageCache.init(testing.allocator, 2, eviction);
    defer cache.deinit();

    var raw_page1 = [_]u8{0x11} ** 128;
    var raw_page2 = [_]u8{0x22} ** 128;
    var raw_page3 = [_]u8{0x33} ** 128;

    const handle1 = PageHandle{ .refs = 0, .page_num = 1, .raw = &raw_page1, .state = .clean };
    const handle2 = PageHandle{ .refs = 0, .page_num = 2, .raw = &raw_page2, .state = .clean };
    const handle3 = PageHandle{ .refs = 0, .page_num = 3, .raw = &raw_page3, .state = .clean };

    try cache.put(handle1);
    try cache.put(handle2);

    const page1_ref = cache.get(1).?;
    try testing.expectEqual(page1_ref.refs, 1);
    try testing.expect(!cache.contains_in_lru(1));

    try cache.put(handle3);

    try testing.expectEqual(test_eviction.counter, 1);
    try testing.expect(cache.contains(1));
    try testing.expect(!cache.contains(2));
    try testing.expect(cache.contains(3));

    cache.release(1);
    try testing.expectEqual(cache.table.get(1).?.data.refs, 0);
    try testing.expect(cache.contains_in_lru(1));
}

test "cache contains" {
    var test_eviction = TestEviction{};
    const eviction = test_eviction.eviction();

    var cache = try PageCache.init(testing.allocator, 2, eviction);
    defer cache.deinit();

    var raw_page = [_]u8{0x42} ** 256;
    const handle = PageHandle{ .refs = 0, .page_num = 1, .raw = &raw_page, .state = .clean };

    try cache.put(handle);

    try testing.expect(cache.contains(1));
    try testing.expect(cache.contains_in_lru(1));

    _ = cache.get(1);

    try testing.expect(cache.contains(1));
    try testing.expect(!cache.contains_in_lru(1));

    cache.release(1);

    try testing.expect(cache.contains(1));
    try testing.expect(cache.contains_in_lru(1));
}
