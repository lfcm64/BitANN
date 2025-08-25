const std = @import("std");
const io = @import("io.zig");
const cache = @import("cache.zig");
const pool = @import("pool.zig");
const pages = @import("pages.zig");
const types = @import("types.zig");

const testing = std.testing;

const PageHandle = cache.PageHandle;
const EvictionCallBack = cache.EvictionCallBack;

const Allocator = std.mem.Allocator;

fn on_cache_eviction(ctx: *anyopaque, handle: *PageHandle) anyerror!void {
    if (handle.state != .dirty) return;
    const pager: *Pager = @ptrCast(@alignCast(ctx));
    try pager.writer.write_page(handle.page_num, handle.page);
}

pub const Pager = struct {
    allocator: Allocator,

    page_count: u32,
    page_size: u32,
    free_list_start: u32,

    reader: io.PageReader,
    writer: io.PageWriter,

    pool: *pool.PagePool,
    cache: *cache.PageCache,

    pub fn init(allocator: Allocator, file: std.fs.File, header: *types.DBHeader) !*Pager {
        const pager = try allocator.create(Pager);
        errdefer allocator.destroy(pager);

        const eviction_callback = EvictionCallBack{
            .ptr = pager,
            .evict_fn = on_cache_eviction,
        };

        const page_pool = try pool.PagePool.init(allocator, header.page_size);
        errdefer page_pool.deinit();

        const page_cache = try cache.PageCache.init(allocator, header.cache_size, eviction_callback);
        errdefer page_cache.deinit();

        pager.* = .{
            .allocator = allocator,
            .page_count = header.page_count,
            .page_size = header.page_size,
            .free_list_start = header.free_list_start,
            .reader = io.PageReader.new(file, header.page_size),
            .writer = io.PageWriter.new(file, header.page_size),
            .pool = page_pool,
            .cache = page_cache,
        };
        return pager;
    }

    pub fn deinit(pager: *Pager) void {
        pager.cache.deinit();
        pager.pool.deinit();
        pager.allocator.destroy(pager);
    }

    pub fn get_page(pager: *Pager, page_num: u32) !*PageHandle {
        if (page_num >= pager.page_count) return error.IndexOutOfBounds;
        try pager.load_page(page_num);
        return pager.cache.get(page_num) orelse error.PageNotFound;
    }

    pub fn load_page(pager: *Pager, page_num: u32) !void {
        if (pager.cache.contains(page_num)) return;

        const page = try pager.pool.acquire();
        errdefer pager.pool.release(page);

        try pager.reader.read_page(page_num, page);

        const handle = PageHandle{ .page_num = page_num, .page = page, .state = .clean };
        try pager.cache.put(handle);
    }

    pub fn alloc_page(pager: *Pager) !*PageHandle {
        const free_page_num = pager.free_list_start;

        if (free_page_num == 0) {
            // No free pages available, allocate a new one
            const new_page_num = pager.page_count + 1;
            const bytes = try pager.pool.acquire();

            const handle = PageHandle{ .page_num = new_page_num, .page = bytes, .state = .not_initialized };
            try pager.cache.put(handle);
            try pager.set_page_count(new_page_num);
            return pager.cache.get(new_page_num) orelse error.PageNotFound;
        } else {
            // Reuse a free page
            const page_handle = try pager.get_page(free_page_num);
            const header = pages.page_header(page_handle.page);
            try pager.free_list_append(header.next_page);
            return page_handle;
        }
    }

    pub fn free_page(pager: *Pager, page_num: u32) !void {
        const page_handle = try pager.get_page(page_num);
        const header = pages.page_header(page_handle.page);

        const current_free = pager.free_list_start;
        header.next_page = current_free;
        try pager.free_list_append(page_num);
        page_handle.state = .not_initialized;
    }

    fn set_page_count(pager: *Pager, page_count: u32) !void {
        const handle = try pager.get_page(0);
        const header = pages.db_header(handle.page);
        header.page_count = page_count;
        pager.page_count = page_count;
        handle.state = .dirty;
    }

    fn free_list_append(pager: *Pager, page_num: u32) !void {
        const handle = try pager.get_page(0);
        const header = pages.db_header(handle.page);
        header.free_list_start = page_num;
        pager.free_list_start = page_num;
        handle.state = .dirty;
    }

    pub fn flush_cache(pager: *Pager) !void {
        try pager.cache.flush();
    }
};

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

test "initialization" {
    const allocator = testing.allocator;
    var header = types.DBHeader{
        .cache_size = 5,
        .page_size = 256,
        .free_list_start = 0,
        .page_count = 10,
    };

    const file = try create_test_file(allocator, 0);
    defer file.close();

    const pager = try Pager.init(allocator, file, &header);
    defer pager.deinit();

    try testing.expectEqual(pager.page_count, header.page_count);
    try testing.expectEqual(pager.page_size, header.page_size);
    try testing.expectEqual(pager.free_list_start, header.free_list_start);
}

test "get_page with valid page number" {
    const allocator = testing.allocator;
    var header = types.DBHeader{
        .cache_size = 5,
        .page_size = 256,
        .free_list_start = 0,
        .page_count = 2,
    };

    const file = try create_test_file(allocator, header.page_size * header.page_count);
    defer file.close();

    const pager = try Pager.init(allocator, file, &header);
    defer pager.deinit();

    const page_handle = try pager.get_page(0);
    try testing.expectEqual(page_handle.page_num, 0);
    try testing.expectEqual(page_handle.page.len, header.page_size);
}

test "get_page with invalid page number" {
    const allocator = testing.allocator;
    var header = types.DBHeader{
        .cache_size = 5,
        .page_size = 256,
        .free_list_start = 0,
        .page_count = 4,
    };

    const file = try create_test_file(allocator, header.page_size * header.page_count);
    defer file.close();

    const pager = try Pager.init(allocator, file, &header);
    defer pager.deinit();

    const result = pager.get_page(header.page_count);
    try testing.expectError(error.IndexOutOfBounds, result);
}

test "load_page caching behavior" {
    const allocator = testing.allocator;
    var header = types.DBHeader{
        .cache_size = 5,
        .page_size = 256,
        .free_list_start = 0,
        .page_count = 10,
    };

    const file = try create_test_file(allocator, header.page_size * header.page_count);
    defer file.close();

    const pager = try Pager.init(allocator, file, &header);
    defer pager.deinit();

    try pager.load_page(1);
    try pager.load_page(1);

    try testing.expect(pager.cache.contains(1));
}

test "alloc_page when no free pages" {
    const allocator = testing.allocator;
    var header = types.DBHeader{
        .cache_size = 5,
        .page_size = 256,
        .free_list_start = 0,
        .page_count = 10,
    };

    const file = try create_test_file(allocator, header.page_size * header.page_count + 5);
    defer file.close();

    const pager = try Pager.init(allocator, file, &header);
    defer pager.deinit();

    const new_page = try pager.alloc_page();
    try testing.expectEqual(new_page.page_num, header.page_count + 1);
    try testing.expectEqual(new_page.state, .not_initialized);
    try testing.expectEqual(pager.page_count, header.page_count + 1);
}

test "free_page functionality" {
    const allocator = testing.allocator;
    var header = types.DBHeader{
        .cache_size = 5,
        .page_size = 256,
        .free_list_start = 0,
        .page_count = 10,
    };

    const file = try create_test_file(allocator, header.page_size * header.page_count);
    defer file.close();

    const pager = try Pager.init(allocator, file, &header);
    defer pager.deinit();

    try pager.free_page(5);

    const page_handle = try pager.get_page(5);
    try testing.expectEqual(pager.free_list_start, 5);
    try testing.expectEqual(page_handle.state, .not_initialized);
}
