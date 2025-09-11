const std = @import("std");
const storage = @import("storage.zig");
const pool = @import("pool.zig");
const cache = @import("cache.zig");
const pages = @import("pages.zig");

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

        return pages.create_page(page_type, raw, page_num, params);
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
