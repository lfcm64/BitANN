const std = @import("std");
const io = @import("io.zig");
const cache = @import("pcache.zig");
const pool = @import("ppool.zig");
const page = @import("page.zig");

const PNode = cache.PNode;
const PEviction = cache.PEviction;

const Page = page.Page;
const PType = page.PType;

const Allocator = std.mem.Allocator;

fn cache_evict(ctx: *anyopaque, pnode: PNode) anyerror!void {
    const pager: *Pager = @ptrCast(@alignOf(ctx));
    try pager.pwriter.writePage(pnode.pnum, pnode.rpage);
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

    pub fn getPage(pager: *Pager, pnum: u32) !Page {
        if (pager.pcache.get(pnum)) |pnode| {
            return pnode;
        }
        const bytes = try pager.ppool.acquire();

        try pager.preader.readPage(pnum, bytes);
        try pager.pcache.put(pnum, bytes);

        const node = pager.pcache.get(pnum) orelse unreachable;
        return Page.new(node);
    }

    pub fn getFreePage(pager: *Pager, page_type: PType) !Page {
        if (try pager.getFirstFreePage()) |fpage| {}
    }

    fn getFirstFreePage(pager: *Pager) !?Page {
        const hpage = try pager.getPage(0);

        if (hpage.nextPage()) |next| {
            const npage = try pager.getPage(next);
            hpage.setNextPage(npage.nextPage());

            return npage;
        }
        return null;
    }

    pub fn flushCache(pager: *Pager) void {
        pager.pcache.flush();
    }
};
