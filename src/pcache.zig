const std = @import("std");

const Allocator = std.mem.Allocator;

const DoublyLinkedList = std.DoublyLinkedList(PHandler);
const LNode = DoublyLinkedList.Node;

const AutoHashMapUnmanaged = std.AutoHashMapUnmanaged(u32, *LNode);

pub const PEviction = struct {
    ptr: *anyopaque,
    evict: *const fn (*anyopaque, handler: PHandler) anyerror!void, // function called every time a node is evicted from the cache
};

pub const PHandler = struct {
    pnum: u32, // page number
    dirty: bool, // wether the cache page differ from the disk page
    rpage: []u8,
};

pub const PCache = struct {
    allocator: Allocator,

    size: usize,

    pmap: AutoHashMapUnmanaged,
    plist: DoublyLinkedList,

    eviction: PEviction,

    pub fn init(allocator: Allocator, size: usize, eviction: PEviction) !*PCache {
        const cache = try allocator.create(PCache);
        cache.* = .{
            .allocator = allocator,
            .size = size,
            .pmap = AutoHashMapUnmanaged{},
            .plist = DoublyLinkedList{},
            .eviction = eviction,
        };
        return cache;
    }

    pub fn deinit(cache: *PCache) void {
        while (cache.plist.pop()) |node| {
            cache.allocator.destroy(node);
        }
        cache.pmap.deinit(cache.allocator);
        cache.allocator.destroy(cache);
    }

    pub fn get(cache: *PCache, pnum: u32) ?*PHandler {
        if (cache.pmap.get(pnum)) |pnode| {
            cache.plist.remove(pnode);
            cache.plist.prepend(pnode);
            return &pnode.data;
        }
        return null;
    }

    pub fn put(cache: *PCache, pnum: u32, rpage: []u8) !void {
        if (cache.pmap.get(pnum)) |pnode| {
            @memcpy(pnode.data.rpage, rpage);
            pnode.data.dirty = true;

            cache.plist.remove(pnode);
            cache.plist.prepend(pnode);
            return;
        }

        if (cache.pmap.count() >= cache.size) {
            try cache.evict();
        }

        const lnode = try cache.allocator.create(DoublyLinkedList.Node);
        lnode.* = .{
            .data = .{
                .pnum = pnum,
                .dirty = false,
                .rpage = rpage,
            },
        };

        cache.plist.prepend(lnode);
        try cache.pmap.put(cache.allocator, pnum, lnode);
    }

    pub fn contains(cache: *PCache, pnum: u32) bool {
        return cache.pmap.contains(pnum);
    }

    pub fn flush(cache: *PCache) !void {
        var it = cache.plist.first;
        while (it) |node| {
            if (node.data.dirty) {
                try cache.eviction.evict(cache.eviction.ptr, node.data);
                node.data.dirty = false;
            }
            it = node.next;
        }
    }

    fn evict(cache: *PCache) !void {
        if (cache.plist.last) |lnode| {
            try cache.eviction.evict(cache.eviction.ptr, lnode.data);

            _ = cache.pmap.remove(lnode.data.pnum);
            cache.plist.remove(lnode);

            cache.allocator.destroy(lnode);
        }
    }
};

// Test helper: Simple eviction counter
const TestEviction = struct {
    counter: u32 = 0,

    fn evict(ptr: *anyopaque, handler: PHandler) anyerror!void {
        _ = handler;
        const cache: *TestEviction = @ptrCast(@alignCast(ptr));
        cache.counter += 1;
    }

    fn getEviction(cache: *TestEviction) PEviction {
        return PEviction{ .ptr = cache, .evict = evict };
    }
};

test "cache put and get" {
    const testing = std.testing;

    var test_eviction = TestEviction{};
    const eviction = test_eviction.getEviction();

    var cache = try PCache.init(testing.allocator, 2, eviction);
    defer cache.deinit();

    var page_data = [_]u8{0x42} ** 256;
    try cache.put(1, &page_data);

    const retrieved = cache.get(1);
    try testing.expect(retrieved != null);
    try testing.expect(retrieved.?.pnum == 1);
    try testing.expect(retrieved.?.dirty == false);
}

test "cache eviction when full" {
    const testing = std.testing;

    var test_eviction = TestEviction{};
    const eviction = test_eviction.getEviction();

    var cache = try PCache.init(testing.allocator, 2, eviction);
    defer cache.deinit();

    var page1 = [_]u8{0x11} ** 128;
    var page2 = [_]u8{0x22} ** 128;
    var page3 = [_]u8{0x33} ** 128;

    try cache.put(1, &page1);
    try cache.put(2, &page2);
    try cache.put(3, &page3); // Should evict page 1

    try testing.expect(test_eviction.counter == 1);
    try testing.expect(cache.get(1) == null);
    try testing.expect(cache.get(2) != null);
    try testing.expect(cache.get(3) != null);
}

test "cache flush dirty pages" {
    const testing = std.testing;

    var test_eviction = TestEviction{};
    const eviction = test_eviction.getEviction();

    var cache = try PCache.init(testing.allocator, 2, eviction);
    defer cache.deinit();

    var page_data = [_]u8{0x42} ** 128;
    try cache.put(1, &page_data);

    var node = cache.get(1).?;
    node.dirty = true;

    try cache.flush();

    try testing.expect(test_eviction.counter == 1);
    try testing.expect(node.dirty == false); // Should be clean after flush
}
