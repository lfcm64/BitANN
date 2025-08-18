const std = @import("std");

pub const Collection = struct {
    id: u32,
    first_cluster_page: u32,

    pub const size = @sizeOf(@This());
};

pub const Cluster = struct {
    position: []f32,
    first_vector_page: u32,

    pub const size = @sizeOf(@This());
};

pub const Vector = struct {
    id: u32,
    position: []f32,
};
