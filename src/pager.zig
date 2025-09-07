const std = @import("std");
const storage = @import("storage.zig");
const pool = @import("pool.zig");
const cache = @import("cache.zig");
const pages = @import("pages.zig");
const types = @import("types.zig");

const testing = std.testing;
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const PagePool = pool.PagePool;

const PageCache = cache.PageCache;
const PageHandle = cache.PageHandle;
const EvictionCallBack = cache.EvictionCallBack;

const RawPage = pages.RawPage;
const Page = pages.Page;
const PageHeader = pages.PageHeader;
const PageType = pages.PageType;
const PageParams = pages.PageParams;

fn on_cache_eviction(ctx: *anyopaque, handle: *PageHandle) anyerror!void {
    const pager: *Pager = @ptrCast(@alignCast(ctx));
    try pager.writer.write_page(handle.page_num, handle.raw);
}

pub const Pager = struct {
    allocator: Allocator,
    meta_page: *pages.MetadataPage,

    reader: storage.Reader,
    writer: storage.Writer,

    pool: *PagePool,
    cache: *PageCache,

    pub fn init(allocator: Allocator, sto: *storage.Storage) !*Pager {
        const pager = try allocator.create(Pager);
        errdefer allocator.destroy(pager);

        const eviction_callback = EvictionCallBack{
            .ptr = pager,
            .on_evict_fn = on_cache_eviction,
        };
        const page_pool = try PagePool.init(allocator, sto.page_size);
        errdefer page_pool.deinit();

        const raw = try page_pool.acquire();

        var reader = sto.reader();
        try reader.read_page(0, raw);

        const meta_page: *pages.MetadataPage = @ptrCast(@alignCast(raw));

        const page_cache = try PageCache.init(allocator, meta_page.meta.cache_size, eviction_callback);
        errdefer page_cache.deinit();

        const handle = PageHandle{ .refs = 1, .page_num = 0, .raw = raw, .state = .dirty };
        try page_cache.put(handle);

        pager.* = .{
            .allocator = allocator,
            .meta_page = meta_page,
            .reader = reader,
            .writer = sto.writer(),
            .pool = page_pool,
            .cache = page_cache,
        };

        _ = try pager.new_page(.collection, .{ .prev_page = 0 });
        return pager;
    }

    pub fn deinit(pager: *Pager) void {
        pager.cache.deinit();
        pager.pool.deinit();
        pager.allocator.destroy(pager);
    }

    pub fn get_page(pager: *Pager, comptime page_type: PageType, page_num: u32) !*Page(page_type) {
        const raw = try get_raw_page(pager, page_num);
        assert(raw[0] == @intFromEnum(page_type));
        assert(std.mem.readInt(u32, raw[1..5], .little) == page_num);
        return @ptrCast(@alignCast(raw));
    }

    pub fn new_page(pager: *Pager, comptime page_type: PageType, params: PageParams(page_type)) !*Page(page_type) {
        const raw = try pager.pool.acquire();
        errdefer pager.pool.release(raw);

        const page_num = pager.meta_page.meta.page_count;

        const handle = PageHandle{ .refs = 1, .page_num = page_num, .raw = raw, .state = .dirty };
        try pager.cache.put(handle);
        pager.meta_page.meta.page_count += 1;

        return Page(page_type).create(raw, page_num, params);
    }

    pub fn release_page(pager: *Pager, ptr: *anyopaque) void {
        const header: *PageHeader = @ptrCast(@alignCast(ptr));
        const node = pager.cache.table.get(header.page_num) orelse @panic("PageNotFound");
        assert(ptr == @as(*anyopaque, node.data.raw.ptr));
        pager.cache.release(node.data.page_num);
    }

    pub fn mark_dirty(pager: *Pager, ptr: *anyopaque) !void {
        const header: *PageHeader = @ptrCast(@alignCast(ptr));
        const node = pager.cache.table.get(header.page_num) orelse @panic("PageNotFound");
        assert(ptr == @as(*anyopaque, node.data.raw.ptr));
        node.data.state == .dirty;
    }

    fn get_raw_page(pager: *Pager, page_num: u32) !RawPage {
        if (pager.cache.get(page_num)) |cached| {
            return cached.raw;
        }
        const raw = try pager.pool.acquire();
        errdefer pager.pool.release(raw);

        try pager.reader.read_page(page_num, raw);

        const handle = PageHandle{ .refs = 1, .page_num = page_num, .raw = raw, .state = .dirty };
        try pager.cache.put(handle);
        return raw;
    }

    pub const FlushOption = enum {
        soft,
        hard,
    };

    pub fn flush_cache(pager: *Pager, opts: FlushOption) !void {
        switch (opts) {
            .soft => try pager.cache.flush(),
            .hard => try pager.cache.flush_hard(),
        }
    }
};

// Helper function to create a temporary test file
fn create_test_file(allocator: std.mem.Allocator, size: usize) !std.fs.File {
    const temp_dir = testing.tmpDir(.{});
    const file = try temp_dir.dir.createFile("test_db", .{ .read = true });

    const zeros = try allocator.alloc(u8, size);
    defer allocator.free(zeros);
    @memset(zeros, 0);
    _ = try file.writeAll(zeros);
    try file.seekTo(0);

    return file;
}

test "initialization" {
    const allocator = testing.allocator;
    const meta = types.Metadata{ .cache_size = 5, .page_size = 256, .page_count = 1 };
    const file = try create_test_file(allocator, meta.page_size * 10);
    defer file.close();

    const pager = try Pager.init(allocator, file, meta, .create_new);
    defer pager.deinit();

    try testing.expectEqual(1, pager.page_count);
    try testing.expect(pager.cache.contains(0));
}

test "new_page" {
    const allocator = testing.allocator;
    const meta = types.Metadata{ .cache_size = 5, .page_size = 256, .page_count = 1 };
    const file = try create_test_file(allocator, meta.page_size * 10);
    defer file.close();

    const pager = try Pager.init(allocator, file, meta, .create_new);
    defer pager.deinit();

    const page = try pager.new_page(.collection, .{ .prev_page = 0 });
    defer pager.release_page(page);

    try testing.expectEqual(1, page.header.page_num);
    try testing.expectEqual(2, pager.page_count);
}

test "get_page increments refs" {
    const allocator = testing.allocator;
    const meta = types.Metadata{ .cache_size = 5, .page_size = 256, .page_count = 1 };
    const file = try create_test_file(allocator, meta.page_size * 10);
    defer file.close();

    const pager = try Pager.init(allocator, file, meta, .create_new);
    defer pager.deinit();

    const new_page = try pager.new_page(.collection, .{ .prev_page = 0 });
    const page_num = new_page.header.page_num;
    pager.release_page(new_page);

    const page1 = try pager.get_page(.collection, page_num);
    const page2 = try pager.get_page(.collection, page_num);
    defer {
        pager.release_page(page1);
        pager.release_page(page2);
    }

    try testing.expectEqual(2, pager.cache.table.get(page_num).?.data.refs);
}

test "release_page decrements refs" {
    const allocator = testing.allocator;
    const meta = types.Metadata{ .cache_size = 5, .page_size = 256, .page_count = 1 };
    const file = try create_test_file(allocator, meta.page_size * 10);
    defer file.close();

    const pager = try Pager.init(allocator, file, meta, .create_new);
    defer pager.deinit();

    const page = try pager.new_page(.collection, .{ .prev_page = 0 });
    const page_num = page.header.page_num;

    try testing.expectEqual(1, pager.cache.table.get(page_num).?.data.refs);
    pager.release_page(page);
    try testing.expectEqual(0, pager.cache.table.get(page_num).?.data.refs);
}
