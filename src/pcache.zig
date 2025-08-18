const std = @import("std");

const Allocator = std.mem.Allocator;

const DoublyLinkedList = std.DoublyLinkedList(PNode);
const LNode = DoublyLinkedList.Node;

const AutoHashMapUnmanaged = std.AutoHashMapUnmanaged(u32, *LNode);

pub const PEviction = struct {
    ptr: *anyopaque,
    evict: *const fn (*anyopaque, node: PNode) anyerror!void, // function called every time a node is evicted from the cache
};

pub const PNode = struct {
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

    pub fn get(self: *PCache, pnum: u32) ?*PNode {
        if (self.pmap.get(pnum)) |pnode| {
            self.plist.remove(pnode);
            self.plist.prepend(pnode);
            return &pnode.data;
        }
        return null;
    }

    pub fn put(self: *PCache, pnum: u32, rpage: []u8) !void {
        if (self.pmap.get(pnum)) |pnode| {
            @memcpy(pnode.data.rpage, rpage);
            pnode.data.dirty = true;

            self.plist.remove(pnode);
            self.plist.prepend(pnode);
            return;
        }

        if (self.pmap.count() >= self.size) {
            try self.evict();
        }

        const lnode = try self.allocator.create(DoublyLinkedList.Node);
        lnode.* = .{
            .data = .{
                .pnum = pnum,
                .dirty = false,
                .rpage = rpage,
            },
        };

        self.plist.prepend(lnode);
        try self.pmap.put(self.allocator, pnum, lnode);
    }

    pub fn flush(self: *PCache) !void {
        var it = self.plist.first;
        while (it) |node| {
            if (node.data.dirty) {
                try self.eviction.evict(self.eviction.ptr, node.data);
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

    fn evict(ptr: *anyopaque, node: PNode) anyerror!void {
        _ = node;
        const self: *TestEviction = @ptrCast(@alignCast(ptr));
        self.counter += 1;
    }

    fn getEviction(self: *TestEviction) PEviction {
        return PEviction{ .ptr = self, .evict = evict };
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
