const std = @import("std");
const io = @import("io.zig");
const pages = @import("pages.zig");
const types = @import("types.zig");

const PagePool = @import("pool.zig");
const PageCache = @import("cache.zig");

const testing = std.testing;
const Allocator = std.mem.Allocator;

const Page = pages.Page;
const PageType = pages.PageType;
const PageInitParams = pages.PageInitParameters;

const EvictionCallBack = PageCache.EvictionCallBack;

fn on_cache_eviction(ctx: *anyopaque, page: *Page) anyerror!void {
    //if (page.state != .dirty) return;
    const pager: *Pager = @ptrCast(@alignCast(ctx));
    try pager.writer.write_page(page.page_num, page.raw);
}

pub const PagerInitMode = enum {
    open_existing,
    create_new,
};

const Pager = @This();

allocator: Allocator,
page_count: u32,
page_size: u32,

reader: io.PageReader,
writer: io.PageWriter,

pool: *PagePool,
cache: *PageCache,

pub fn init(allocator: Allocator, file: std.fs.File, meta: types.Metadata, mode: PagerInitMode) !*Pager {
    const pager = try allocator.create(Pager);
    errdefer allocator.destroy(pager);

    const eviction_callback = EvictionCallBack{
        .ptr = pager,
        .evict_fn = on_cache_eviction,
    };

    const page_pool = try PagePool.init(allocator, meta.page_size);
    errdefer page_pool.deinit();

    const page_cache = try PageCache.init(allocator, meta.cache_size, eviction_callback);
    errdefer page_cache.deinit();

    pager.* = .{
        .allocator = allocator,
        .page_count = meta.page_count,
        .page_size = meta.page_size,
        .reader = io.PageReader.new(file, meta.page_size),
        .writer = io.PageWriter.new(file, meta.page_size),
        .pool = page_pool,
        .cache = page_cache,
    };

    if (mode == .create_new) {
        const raw = try pager.pool.acquire();
        _ = pages.MetadataPage.new(raw, meta);

        const page = Page{ .page_num = 0, .raw = raw, .state = .dirty };
        try pager.cache.put(page);

        _ = try pager.new_page(.collection, .{ .prev_page = 0 });
    }
    return pager;
}

pub fn deinit(pager: *Pager) void {
    pager.cache.deinit();
    pager.pool.deinit();
    pager.allocator.destroy(pager);
}

pub fn get_page(pager: *Pager, comptime page_type: PageType, page_num: u32) !*pages.TypeToPage(page_type) {
    const page = try pager.get_page_internal(page_num);
    return page.unwrap(page_type);
}

pub fn get_page_header(pager: *Pager, page_num: u32) !*pages.PageHeader {
    const page = try pager.get_page_internal(page_num);
    return page.header();
}

pub fn new_page(pager: *Pager, comptime page_type: PageType, params: PageInitParams(page_type)) !*pages.TypeToPage(page_type) {
    const page = try pager.alloc_page();
    page.mark_dirty();

    const prev_page_num = switch (page_type) {
        .metadata => 0,
        else => params.prev_page,
    };
    if (prev_page_num != 0) {
        const header = try pager.get_page_header(prev_page_num);
        header.next_page = page.page_num;
    }
    return switch (page_type) {
        .metadata => pages.MetadataPage.new(page.raw, params),
        .collection => pages.CollectionPage.new(page.raw, page.page_num, params.prev_page),
        .cluster => pages.ClusterPage.new(page.raw, page.page_num, params.prev_page, params.centroid_dim),
        .vector => pages.VectorPage.new(page.raw, page.page_num, params.prev_page, params.vector_dim),
    };
}

fn get_page_internal(pager: *Pager, page_num: u32) !*Page {
    if (page_num >= pager.page_count) return error.IndexOutOfBounds;
    try pager.load_page(page_num);
    return pager.cache.get(page_num) orelse error.PageNotFound;
}

fn load_page(pager: *Pager, page_num: u32) !void {
    if (pager.cache.contains(page_num)) return;

    const raw = try pager.pool.acquire();
    errdefer pager.pool.release(raw);

    try pager.reader.read_page(page_num, raw);

    const page = Page{ .page_num = page_num, .raw = raw, .state = .clean };
    try pager.cache.put(page);
}

fn alloc_page(pager: *Pager) !*Page {
    const new_page_num = pager.page_count;
    const raw = try pager.pool.acquire();

    const page = Page{ .page_num = new_page_num, .raw = raw, .state = .not_initialized };
    try pager.cache.put(page);
    try pager.set_page_count(new_page_num + 1);
    return pager.cache.get(new_page_num) orelse error.PageNotFound;
}

fn set_page_count(pager: *Pager, page_count: u32) !void {
    pager.page_count = page_count;
    const meta_page = try pager.get_page(.metadata, 0);
    meta_page.meta.page_count = page_count;
}

pub fn flush_cache(pager: *Pager) !void {
    try pager.cache.flush();
}

// Helper function to create a temporary test file
fn create_test_file(allocator: std.mem.Allocator, size: usize) !std.fs.File {
    const temp_dir = testing.tmpDir(.{});
    const file = try temp_dir.dir.createFile("test_db", .{ .read = true });

    // Initialize file with zeros
    const zeros = try allocator.alloc(u8, size);
    defer allocator.free(zeros);
    @memset(zeros, 0);
    _ = try file.writeAll(zeros);
    try file.seekTo(0);

    return file;
}

// Test suite with corrections
test "initialization" {
    const allocator = testing.allocator;
    const meta = types.Metadata{
        .cache_size = 5,
        .page_size = 256,
        .page_count = 10,
    };

    const file = try create_test_file(allocator, meta.page_size * meta.page_count);
    defer file.close();

    const pager = try Pager.init(allocator, file, meta);
    defer pager.deinit();

    try testing.expectEqual(meta.page_count, pager.page_count);
    try testing.expectEqual(meta.page_size, pager.page_size);
}

test "get_page with valid page number" {
    const allocator = testing.allocator;
    const meta = types.Metadata{
        .cache_size = 5,
        .page_size = 256,
        .page_count = 2,
    };

    const file = try create_test_file(allocator, meta.page_size * meta.page_count);
    defer file.close();

    const pager = try Pager.init(allocator, file, meta);
    defer pager.deinit();

    const page = try pager.get_page_internal(0);
    try testing.expectEqual(@as(u32, 0), page.page_num);
    try testing.expectEqual(meta.page_size, page.raw.len);
}

test "get_page with invalid page number" {
    const allocator = testing.allocator;
    const meta = types.Metadata{
        .cache_size = 5,
        .page_size = 256,
        .page_count = 4,
    };

    const file = try create_test_file(allocator, meta.page_size * meta.page_count);
    defer file.close();

    const pager = try Pager.init(allocator, file, meta);
    defer pager.deinit();

    const result = pager.get_page_internal(meta.page_count);
    try testing.expectError(error.IndexOutOfBounds, result);
}

test "load_page caching behavior" {
    const allocator = testing.allocator;
    const meta = types.Metadata{
        .cache_size = 5,
        .page_size = 256,
        .page_count = 10,
    };

    const file = try create_test_file(allocator, meta.page_size * meta.page_count);
    defer file.close();

    const pager = try Pager.init(allocator, file, meta);
    defer pager.deinit();

    try pager.load_page(1);
    try pager.load_page(1); // Should not reload from disk

    try testing.expect(pager.cache.contains(1));
}

test "alloc_page functionality" {
    const allocator = testing.allocator;
    const meta = types.Metadata{
        .cache_size = 5,
        .page_size = 256,
        .page_count = 10,
    };

    const file = try create_test_file(allocator, meta.page_size * (meta.page_count + 5));
    defer file.close();

    const pager = try Pager.init(allocator, file, meta);
    defer pager.deinit();

    const initial_page_count = pager.page_count;
    const page = try pager.alloc_page();

    try testing.expectEqual(initial_page_count, page.page_num);
    try testing.expectEqual(pages.PageState.not_initialized, page.state);
    try testing.expectEqual(initial_page_count + 1, pager.page_count);
}

test "flush_cache" {
    const allocator = testing.allocator;
    const meta = types.Metadata{
        .cache_size = 5,
        .page_size = 256,
        .page_count = 10,
    };

    const file = try create_test_file(allocator, meta.page_size * meta.page_count);
    defer file.close();

    const pager = try Pager.init(allocator, file, meta);
    defer pager.deinit();

    // Load some pages
    try pager.load_page(1);
    try pager.load_page(2);

    // This should write any dirty pages to disk
    try pager.flush_cache();
}
