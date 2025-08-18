const std = @import("std");
const types = @import("types.zig");
const cache = @import("pcache.zig");

const Collection = types.Collection;
const Cluster = types.Cluster;
const Vector = types.Vector;

const PNode = cache.PNode;

const page_header_size = 10;

pub const PType = enum {
    Header,
    Collection,
    Cluster,
    Vector,
};

pub const PStatus = enum {
    Used,
    Free,
};

pub const HeaderPage = struct {
    pnode: *PNode,

    pub fn magic(page: *HeaderPage) [6]u8 {
        var result: [6]u8 = undefined;
        @memcpy(result[0..6], page.pnode.rpage[10..16]);
        return result;
    }

    pub fn version(page: *HeaderPage) u16 {
        const bytes = page.pnode.rpage[16..18];
        return std.mem.readInt(u16, bytes[0..2], .little);
    }

    pub fn pageSize(page: *HeaderPage) u32 {
        const bytes = page.pnode.rpage[18..22];
        return std.mem.readInt(u32, bytes[0..4], .little);
    }

    pub fn pageCount(page: *HeaderPage) u32 {
        const bytes = page.pnode.rpage[22..26];
        return std.mem.readInt(u32, bytes[0..4], .little);
    }

    pub fn setPageCount(page: *HeaderPage, count: u32) void {
        std.mem.writeInt(u32, page.pnode.rpage[22..26], count, .little);
    }

    pub fn firstCollectionPage(page: *HeaderPage) u32 {
        const bytes = page.pnode.rpage[26..30];
        return std.mem.readInt(u32, bytes[0..4], .little);
    }

    pub fn firstFreePage(page: *HeaderPage) u32 {
        const bytes = page.pnode.rpage[30..34];
        return std.mem.readInt(u32, bytes[0..4], .little);
    }

    pub fn setFirstFreePage(page: *HeaderPage, pnum: u32) void {
        std.mem.writeInt(u32, page.pnode.rpage[30..34], pnum, .little);
    }

    //pub fn verify() void {}
};

pub const CollectionPage = struct {
    pnode: *PNode,

    const collections_offset = page_header_size + @sizeOf(u32);

    pub fn initBlank(node: *PNode) CollectionPage {
        pnode.rpage[0] = @intFromEnum(ptype);
        const page = init(pnode);
    }

    pub fn count(page: *CollectionPage) u32 {
        const bytes = page.pnode.rpage[14..18];
        return std.mem.readInt(u32, bytes[0..4], .little);
    }

    pub fn setCount(page: *CollectionPage, collection_count: u32) void {
        std.mem.writeInt(u32, page.pnode.rpage[14..18], collection_count, .little);
    }

    pub fn getById(page: *CollectionPage, id: u32) ?Collection {
        const collection_count = page.count();
        for (0..collection_count) |i| {
            if (page.get(i)) |collection| {
                if (collection.id == id) return collection;
            }
        }
        return null;
    }

    pub fn get(page: *CollectionPage, n: usize) ?Collection {
        const collection_count = page.count();
        if (n >= collection_count) return null;

        const offset = collections_offset + n * Collection.size;

        return Collection{
            .id = std.mem.readInt(u32, page.pnode.rpage[offset..][0..4], .little),
            .first_cluster_page = std.mem.readInt(u32, page.pnode.rpage[offset + 4 ..][0..4], .little),
        };
    }

    pub fn add(page: *CollectionPage, collection: Collection) !void {
        const current_count = page.count();

        if (page.remainingSpace() < Collection.size) {
            return error.InsufficientSpace;
        }
        const offset = collections_offset + page.count() * Collection.size;
        std.mem.writeInt(u32, page.pnode.rpage[offset..][0..4], collection.id, .little);
        std.mem.writeInt(u32, page.pnode.rpage[offset + 4 ..][0..4], collection.first_cluster_page, .little);

        page.setCount(current_count + 1);
    }

    pub fn remainingSpace(page: *CollectionPage) usize {
        const used_space = collections_offset + page.count() * Collection.size;
        return page.pnode.psize - used_space;
    }
};

pub const ClusterPage = struct {
    pnode: *PNode,

    const clusters_offset = page_header_size + @sizeOf(u32);

    pub fn count(page: *ClusterPage) u32 {
        const bytes = page.pnode.rpage[14..18];
        return std.mem.readInt(u32, bytes[0..4], .little);
    }

    pub fn setCount(page: *ClusterPage, item_count: u32) void {
        std.mem.writeInt(u32, page.pnode.rpage[14..18], item_count, .little);
    }

    pub fn get(page: *ClusterPage, n: usize) ?Vector {
        const cluster_count = page.count();
        if (n >= cluster_count) return null;

        const offset = clusters_offset + n * page.vectorSize();

        const bytes = page.pnode.rpage[offset..][0 .. page.dimension() * @sizeOf(f32)];
        const position = std.mem.bytesAsSlice(f32, bytes);

        const first_vector_page_offset = offset + bytes.len;
        const first_vector_page = std.mem.readInt(u32, page.pnode.rpage[first_vector_page_offset..][0..4], .little);

        return Cluster{
            .position = position,
            .first_vector_page = first_vector_page,
        };
    }

    pub fn add(page: *ClusterPage, cluster: Cluster) !void {
        const current_count = page.count();
        if (page.remainingSpace() < page.clusterSize()) {
            return error.InsufficientSpace;
        }

        const offset = clusters_offset + page.count() * page.clusterSize();

        const position_bytes = std.mem.sliceAsBytes(cluster.position);
        const dest_bytes = page.pnode.rpage[offset..][0 .. page.dimension() * @sizeOf(f32)];
        @memcpy(dest_bytes, position_bytes);

        const first_vector_page_offset = offset + (page.dimension() * @sizeOf(f32));
        std.mem.writeInt(u32, page.pnode.rpage[first_vector_page_offset..][0..4], cluster.first_vector_page, .little);

        page.setCount(current_count + 1);
    }

    pub fn remainingSpace(page: *ClusterPage) usize {
        const used_space = clusters_offset + page.count() * Cluster.size;
        return page.pnode.psize - used_space;
    }
};

pub const VectorPage = struct {
    pnode: *PNode,

    const vectors_offset = page_header_size + 2 * @sizeOf(u32);

    pub fn count(page: *VectorPage) u32 {
        const bytes = page.pnode.rpage[14..18];
        return std.mem.readInt(u32, bytes[0..4], .little);
    }

    pub fn dimension(page: *VectorPage) u32 {
        const bytes = page.pnode.rpage[18..22];
        return std.mem.readInt(u32, bytes[0..4], .little);
    }

    pub fn setCount(page: *VectorPage, vec_count: u32) void {
        std.mem.writeInt(u32, page.pnode.rpage[14..18], vec_count, .little);
    }

    pub fn getById(page: *VectorPage, id: u32) ?Vector {
        const vec_count = page.count();
        for (0..vec_count) |i| {
            if (page.get(i)) |vec| {
                if (vec.id == id) return vec;
            }
        }
        return null;
    }

    pub fn get(page: *VectorPage, n: usize) ?Vector {
        const vec_count = page.count();
        if (n >= vec_count) return null;

        const offset = vectors_offset + n * page.vectorSize();

        const position_bytes = page.pnode.rpage[offset + @sizeOf(u32) ..][0 .. page.dimension() * @sizeOf(f32)];
        const position = std.mem.bytesAsSlice(f32, position_bytes);

        return Vector{
            .id = std.mem.readInt(u32, page.pnode.rpage[offset..][0..4], .little),
            .position = position,
        };
    }

    pub fn add(page: *VectorPage, vec: Vector) !void {
        const current_count = page.count();

        if (page.remainingSpace() < page.vectorSize()) {
            return error.InsufficientSpace;
        }
        const offset = vectors_offset + page.count() * page.vectorSize();

        std.mem.writeInt(u32, page.pnode.rpage[offset..][0..4], vec.id, .little);

        const position_bytes = std.mem.sliceAsBytes(vec.position);
        const dest_bytes = page.pnode.rpage[offset + @sizeOf(u32) ..][0 .. page.dimension() * @sizeOf(f32)];
        @memcpy(dest_bytes, position_bytes);

        page.setCount(current_count + 1);
    }

    pub fn remainingSpace(page: *VectorPage) usize {
        const used_space = vectors_offset + page.count() * page.vectorSize();
        return page.pnode.psize - used_space;
    }

    pub fn vectorSize(page: *VectorPage) u32 {
        return @sizeOf(u32) + page.dimension() * @sizeOf(f32);
    }
};

pub const Page = union(PType) {
    Header: HeaderPage,
    Collection: CollectionPage,
    Cluster: ClusterPage,
    Vector: VectorPage,

    pub fn init(pnode: *PNode) Page {
        const ptype: PType = @enumFromInt(pnode.rpage[0]);

        return switch (ptype) {
            .Header => Page{ .Header = .{ .pnode = pnode } },
            .Collection => Page{ .Collection = .{ .pnode = pnode } },
            .Cluster => Page{ .Cluster = .{ .pnode = pnode } },
            .Vector => Page{ .Vector = .{ .pnode = pnode } },
        };
    }

    pub fn initBlank(pnode: *PNode, ptype: PType) Page {
        pnode.rpage[0] = @intFromEnum(ptype);
        const page = init(pnode);

        page.setNextPage(null);
        page.setStatus(.Used);

        switch (ptype) {
            .Header => unreachable,
            .Collection => page.Collection.setCount(0),
            .Cluster => page.Cluster.setCount(0),
            .Vector => Page{ .Vector = .{ .pnode = pnode } },
        }
        return page;
    }

    pub fn pageType(page: *Page) PType {
        const rpage = page.rawPage();
        return @enumFromInt(rpage[0]);
    }

    pub fn pageNumber(page: *Page) u32 {
        const rpage = page.rawPage();
        const bytes = rpage[1..5];
        return std.mem.readInt(u32, bytes[0..4], .little);
    }

    pub fn status(page: *Page) PStatus {
        const rpage = page.rawPage();
        return @enumFromInt(rpage[5]);
    }

    pub fn setStatus(page: *Page, pstatus: PStatus) PStatus {
        const rpage = page.rawPage();
        rpage[5] = @intFromEnum(pstatus);
    }

    pub fn nextPage(page: *Page) ?u32 {
        const rpage = page.rawPage();
        const bytes = rpage[6..10];
        const npage = std.mem.readInt(u32, bytes[0..4], .little);
        return if (npage == 0) null else npage;
    }

    pub fn setNextPage(page: *Page, next: ?u32) void {
        const rpage = page.rawPage();
        const next_page = next orelse 0;
        std.mem.writeInt(u32, rpage[6..10], next_page, .little);
    }

    fn rawPage(page: *Page) []u8 {
        return switch (page.*) {
            .Header => |*hp| hp.pnode.rpage,
            .Collection => |*cp| cp.pnode.rpage,
            .Cluster => |*clp| clp.pnode.rpage,
            .Vector => |*vp| vp.pnode.rpage,
        };
    }
};
