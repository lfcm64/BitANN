pub const Metadata = struct {
    magic: [6]u8 = [6]u8{ 'a', 'b', 'c', 'd', 'e', 'f' },
    version: u16 = 1,
    page_size: u32 = 4096,
    page_count: u32 = 0,
    first_collection_page: u32 = 1,
    free_list_start: u32 = 0,
    cache_size: u32 = 1024,
};

pub const Collection = packed struct {
    id: u32,
    first_cluster_page: u32,
    vector_count: u32,
    vector_dimension: u32,
};

pub const Cluster = struct {
    first_vector_page: u32,
    vector_count: u32,
    centroid: []f32,
};

pub const Vector = struct {
    id: u32,
    point: []f32,
};
