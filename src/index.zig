const std = @import("std");
const types = @import("types.zig");

const Pager = @import("pager.zig");

const Collection = types.Collection;
const Cluster = types.Cluster;
const Vector = types.Vector;

const Allocator = std.mem.Allocator;

const Index = @This();

collection: Collection,
page_size: u32,

pager: *Pager,

pub fn init(allocator: Allocator, pager: *Pager, page_size: u32, id: u32, vector_dim: u32) !Index {
    const collection = Collection{ .id = id, .first_cluster_page = 0, .vector_count = 0, .vector_dim = vector_dim };
    var self = Index{ .pager = pager, .page_size = page_size, .collection = collection };

    try self.add_collection(collection);

    const cluster_page = try pager.new_page(.cluster, .{
        .prev_page = 0,
        .centroid_dim = vector_dim,
    });
    self.collection.first_cluster_page = cluster_page.header.page_num;

    const vector_page = try pager.new_page(.vector, .{ .prev_page = 0, .vector_dim = collection.vector_dim });

    const centroid = try allocator.alloc(f32, vector_dim);
    @memset(centroid, 0.0);

    const cluster = Cluster{ .vector_count = 0, .first_vector_page = vector_page.header.page_num, .centroid = centroid };
    try cluster_page.add_cluster(cluster, page_size);

    return self;
}

pub fn add_collection(index: *Index, collection: Collection) !void {
    const page_num = try index.traverse_to_last_page(1);
    const collec_page = try index.pager.get_page(.collection, page_num);

    collec_page.add_collection(collection, index.page_size) catch {
        const new_collec_page = try index.pager.new_page(.collection, .{ .prev_page = page_num });
        new_collec_page.add_collection(collection, index.page_size) catch unreachable;
    };
}

pub fn add_vector(index: *Index, vector: Vector) !void {
    const page_num = try index.find_last_vector_page();
    const vec_page = try index.pager.get_page(.vector, page_num);

    vec_page.add_vector(vector, index.page_size) catch {
        const new_vec_page = try index.pager.new_page(.vector, .{
            .prev_page = page_num,
            .vector_dim = index.collection.vector_dim,
        });
        new_vec_page.add_vector(vector, index.page_size) catch unreachable;
    };
}

fn find_last_vector_page(index: *Index) !u32 {
    const first_page_num = try index.find_first_vector_page();
    return index.traverse_to_last_page(first_page_num);
}

fn find_first_vector_page(index: *Index) !u32 {
    const cluster_page = try index.pager.get_page(.cluster, index.collection.first_cluster_page);
    const cluster = try cluster_page.get_cluster(0);

    return cluster.first_vector_page;
}

fn traverse_to_last_page(index: *Index, start_page_num: u32) !u32 {
    var header = try index.pager.get_page_header(start_page_num);

    while (true) {
        if (header.next_page == 0) return header.page_num;
        header = try index.pager.get_page_header(header.next_page);
    }
}
