const std = @import("std");
const assert = std.debug.assert;

const AutoHashMap = std.AutoHashMap;
const DoublyLinkedList = std.DoublyLinkedList;
const Allocator = std.mem.Allocator;

fn KeyValue(comptime T: type) type {
    return struct {
        key: u32,
        value: T,
    };
}

pub fn LruCache(comptime T: type) type {
    return struct {
        const Cache = @This();
        const KV = KeyValue(T);
        const Node = DoublyLinkedList(KV).Node;

        allocator: Allocator,
        list: DoublyLinkedList(KV),
        map: AutoHashMap(u32, *Node),
        capacity: usize,

        pub fn init(allocator: Allocator, capacity: usize) Cache {
            return Cache{
                .allocator = allocator,
                .list = DoublyLinkedList(KV){},
                .map = AutoHashMap(u32, *Node).init(allocator),
                .capacity = capacity,
            };
        }

        pub fn deinit(cache: *Cache) void {
            while (cache.list.pop()) |node| {
                cache.allocator.destroy(node);
            }
            cache.map.deinit();
        }

        pub fn get(cache: *Cache, key: u32) ?T {
            if (cache.map.get(key)) |node| {
                cache.list.remove(node);
                cache.list.prepend(node);
                return node.data.value;
            }
            return null;
        }

        pub fn put(cache: *Cache, key: u32, value: T) !void {
            if (cache.map.get(key)) |node| {
                node.data.value = value;
                cache.list.remove(node);
                cache.list.prepend(node);
                return;
            }

            var node_to_use: *Node = undefined;

            if (cache.map.count() >= cache.capacity and cache.capacity > 0) {
                const node = cache.list.pop().?;
                _ = cache.map.remove(node.data.key);
                node_to_use = node;
            } else {
                node_to_use = try cache.allocator.create(Node);
            }

            node_to_use.data = KV{ .key = key, .value = value };
            node_to_use.next = null;
            node_to_use.prev = null;

            cache.list.prepend(node_to_use);
            try cache.map.put(key, node_to_use);
        }

        pub fn remove(cache: *Cache, key: u32) bool {
            if (cache.map.fetchRemove(key)) |entry| {
                const node = entry.value;
                cache.list.remove(node);
                cache.allocator.destroy(node);
                return true;
            }
            return false;
        }

        pub fn size(cache: *Cache) usize {
            return cache.map.count();
        }

        pub fn clear(cache: *Cache) void {
            while (cache.list.pop()) |node| {
                cache.allocator.destroy(node);
            }
            cache.map.clearAndFree();
        }
    };
}
test "Put and get operations" {
    const testing = std.testing;
    var cache = LruCache(u32).init(testing.allocator, 2);
    defer cache.deinit();

    try cache.put(1, 10);
    try cache.put(2, 20);

    try testing.expect(cache.get(1).? == 10);
    try testing.expect(cache.get(2).? == 20);
    try testing.expect(cache.size() == 2);
}

test "Eviction on capacity limit" {
    const testing = std.testing;
    var cache = LruCache(u32).init(testing.allocator, 2);
    defer cache.deinit();

    try cache.put(1, 10);
    try cache.put(2, 20);
    try cache.put(3, 30); // Should evict key 1

    try testing.expect(cache.get(1) == null);
    try testing.expect(cache.get(2).? == 20);
    try testing.expect(cache.get(3).? == 30);
    try testing.expect(cache.size() == 2);
}

test "Access updates recency" {
    const testing = std.testing;
    var cache = LruCache([]const u8).init(testing.allocator, 3);
    defer cache.deinit();

    try cache.put(1, "one");
    try cache.put(2, "two");
    try cache.put(3, "three");

    // Access key 1 to make it most recently used
    _ = cache.get(1);

    // Add key 4, should evict key 2
    try cache.put(4, "four");

    try testing.expect(cache.get(1) != null);
    try testing.expect(cache.get(2) == null);
    try testing.expect(cache.get(3) != null);
    try testing.expect(cache.get(4) != null);
}

test "Update and remove operations" {
    const testing = std.testing;
    var cache = LruCache(u32).init(testing.allocator, 2);
    defer cache.deinit();

    try cache.put(1, 10);
    try cache.put(2, 20);

    // Update existing key
    try cache.put(2, 22);
    try testing.expect(cache.get(2).? == 22);

    try testing.expect(cache.remove(2) == true);
    try testing.expect(cache.get(2) == null);
    try testing.expect(cache.remove(2) == false);
    try testing.expect(cache.size() == 1);
}
