const std = @import("std");
const pages = @import("pages.zig");

const testing = std.testing;

const Page = pages.Page;

const Allocator = std.mem.Allocator;

/// Called every time a page handle is evicted from the cache
pub const EvictionCallBack = struct {
    ptr: *anyopaque,
    evict_fn: *const fn (*anyopaque, *PageHandle) anyerror!void,

    pub fn evict(cb: *EvictionCallBack, handle: *PageHandle) !void {
        try cb.evict_fn(cb.ptr, handle);
    }
};

pub const PageHandleState = enum {
    not_initialized,
    clean,
    dirty,
};

pub const PageHandle = struct {
    page_num: u32,
    page: Page,
    state: PageHandleState,
};

pub const PageCache = struct {
    const DoublyLinkedList = std.DoublyLinkedList(PageHandle);
    const AutoHashMapUnmanaged = std.AutoHashMapUnmanaged(u32, *Node);

    const Node = DoublyLinkedList.Node;

    allocator: Allocator,

    size: usize,

    page_map: AutoHashMapUnmanaged,
    page_list: DoublyLinkedList,

    eviction_cb: EvictionCallBack,

    pub fn init(allocator: Allocator, size: usize, eviction: EvictionCallBack) !*PageCache {
        const cache = try allocator.create(PageCache);
        cache.* = .{
            .allocator = allocator,
            .size = size,
            .page_map = AutoHashMapUnmanaged{},
            .page_list = DoublyLinkedList{},
            .eviction_cb = eviction, // Fixed field name
        };
        return cache;
    }

    pub fn deinit(cache: *PageCache) void {
        while (cache.page_list.pop()) |node| {
            cache.allocator.destroy(node);
        }
        cache.page_map.deinit(cache.allocator);
        cache.allocator.destroy(cache);
    }

    pub fn get(cache: *PageCache, page_num: u32) ?*PageHandle {
        if (cache.page_map.get(page_num)) |node| {
            cache.page_list.remove(node);
            cache.page_list.prepend(node);
            return &node.data;
        }
        return null;
    }

    pub fn put(cache: *PageCache, handle: PageHandle) !void {
        if (cache.page_map.get(handle.page_num)) |node| {
            cache.page_list.remove(node);
            cache.page_list.prepend(node);
            return;
        }

        if (cache.page_map.count() >= cache.size) {
            try cache.evict_last();
        }

        const node = try cache.allocator.create(DoublyLinkedList.Node);
        node.* = .{ .data = handle };

        cache.page_list.prepend(node);
        try cache.page_map.put(cache.allocator, handle.page_num, node);
    }

    pub fn contains(cache: *PageCache, page_num: u32) bool {
        return cache.page_map.contains(page_num);
    }

    pub fn flush(cache: *PageCache) !void {
        while (cache.page_map.size > 0) {
            try cache.evict_last();
        }
    }

    fn evict_last(cache: *PageCache) !void {
        if (cache.page_list.last) |node| {
            try cache.eviction_cb.evict(&node.data);

            _ = cache.page_map.remove(node.data.page_num);
            cache.page_list.remove(node);

            cache.allocator.destroy(node);
        }
    }
};

// Test helper: Simple eviction counter
const TestEviction = struct {
    counter: u32 = 0,

    fn evict(ptr: *anyopaque, handle: *PageHandle) anyerror!void {
        _ = handle;
        const self: *TestEviction = @ptrCast(@alignCast(ptr));
        self.counter += 1;
    }

    fn getEviction(self: *TestEviction) EvictionCallBack {
        return EvictionCallBack{ .ptr = self, .evict_fn = evict };
    }
};

test "cache put and get" {
    var test_eviction = TestEviction{};
    const eviction = test_eviction.getEviction();

    var cache = try PageCache.init(testing.allocator, 2, eviction);
    defer cache.deinit();

    var page_data = [_]u8{0x42} ** 256;
    const handle = PageHandle{
        .page_num = 1,
        .page = &page_data,
        .state = .clean,
    };
    try cache.put(handle);

    const retrieved = cache.get(1);
    try testing.expect(retrieved != null);
    try testing.expectEqual(retrieved.?.page_num, 1);
    try testing.expectEqual(retrieved.?.state, .clean);
}

test "cache eviction when full" {
    var test_eviction = TestEviction{};
    const eviction = test_eviction.getEviction();

    var cache = try PageCache.init(testing.allocator, 2, eviction);
    defer cache.deinit();

    var page1 = [_]u8{0x11} ** 128;
    var page2 = [_]u8{0x22} ** 128;
    var page3 = [_]u8{0x33} ** 128;

    const handle1 = PageHandle{ .page_num = 1, .page = &page1, .state = .clean };
    const handle2 = PageHandle{ .page_num = 2, .page = &page2, .state = .clean };
    const handle3 = PageHandle{ .page_num = 3, .page = &page3, .state = .clean };

    try cache.put(handle1);
    try cache.put(handle2);
    try cache.put(handle3); // Should evict page 1

    try testing.expect(test_eviction.counter == 1);
    try testing.expectEqual(cache.get(1), null);
    try testing.expect(cache.get(2) != null);
    try testing.expect(cache.get(3) != null);
}

test "cache flush" {
    var test_eviction = TestEviction{};
    const eviction = test_eviction.getEviction();

    var cache = try PageCache.init(testing.allocator, 5, eviction);
    defer cache.deinit();

    var page1 = [_]u8{0x11} ** 128;
    var page2 = [_]u8{0x22} ** 128;
    var page3 = [_]u8{0x33} ** 128;

    const handle1 = PageHandle{ .page_num = 1, .page = &page1, .state = .clean };
    const handle2 = PageHandle{ .page_num = 2, .page = &page2, .state = .clean };
    const handle3 = PageHandle{ .page_num = 3, .page = &page3, .state = .clean };

    try cache.put(handle1);
    try cache.put(handle2);
    try cache.put(handle3);

    try cache.flush();

    try testing.expectEqual(test_eviction.counter, 3);
    try testing.expectEqual(cache.page_map.size, 0);
}
