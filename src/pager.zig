const std = @import("std");
const io = @import("io.zig");
const cache = @import("pcache.zig");
const pool = @import("ppool.zig");
const pages = @import("pages.zig");
const types = @import("types.zig");

const PHandler = cache.PHandler;
const PEviction = cache.PEviction;

const Allocator = std.mem.Allocator;

fn cache_evict(ctx: *anyopaque, handler: *PHandler) anyerror!void {
    const pager: *Pager = @ptrCast(@alignOf(ctx));
    try pager.pwriter.writePage(handler.pnum, handler.rpage);
}

pub const Pager = struct {
    allocator: Allocator,

    pcount: usize,
    psize: usize,

    preader: io.PReader,
    pwriter: io.PWriter,

    ppool: *pool.PPool,
    pcache: *cache.PCache,

    pub fn init(allocator: Allocator, file: std.fs.File, csize: usize, pcount: usize, psize: usize) !*Pager {
        const pager = try allocator.create(Pager);

        const p_eviction = PEviction{
            .ptr = pager,
            .evict = cache_evict,
        };

        pager.* = .{
            .allocator = allocator,
            .pcount = pcount,
            .psize = psize,
            .preader = io.PReader.new(file, psize),
            .pwriter = io.PWriter.new(file, psize),
            .ppool = try pool.PPool.init(allocator, psize),
            .pcache = try cache.PCache.init(allocator, csize, p_eviction),
        };
    }

    pub fn deinit(pager: *Pager) void {
        pager.pcache.deinit();
        pager.ppool.deinit();
        pager.allocator.destroy(pager);
    }

    pub fn get_meta_page(pager: *Pager) !*PHandler {
        return pager.get_page(0);
    }

    pub fn get_page(pager: *Pager, page_num: u32) !*PHandler {
        try pager.load_page(page_num);
        return pager.pcache.get(page_num) orelse error.PageNotFound;
    }

    pub fn alloc_page(pager: *Pager) !*PHandler {
        const free_page_num = pager.first_free_page();

        if (free_page_num == 0) {
            const bytes = try pager.ppool.acquire();
            try pager.pcache.put(free_page_num + 1, bytes);

            return pager.pcache.get(free_page_num + 1) orelse error.PageNotFound;
        } else {
            const free_page = try pager.get_page(free_page_num);
            const header: *types.PageHeader = @ptrCast(@alignCast(free_page.rpage));

            const meta_page_handler = try pager.get_meta_page();
            const meta_page: *pages.MetaPage = @ptrCast(@alignCast(meta_page_handler.rpage));
            meta_page.meta.first_free_page = header.next_page;
            return free_page;
        }
    }

    pub fn first_free_page(pager: *Pager) u32 {
        const meta_page_handler = try pager.get_meta_page();
        const meta_page: *pages.MetaPage = @ptrCast(@alignCast(meta_page_handler.rpage));
        return meta_page.meta.first_free_page;
    }

    pub fn load_page(pager: *Pager, page_num: u32) !void {
        if (pager.pcache.contains(page_num)) return;

        const page = try pager.ppool.acquire();
        try pager.preader.readPage(page_num, page);

        try pager.pcache.put(page_num, page);
    }

    pub fn flush_cache(pager: *Pager) void {
        pager.pcache.flush();
    }
};
