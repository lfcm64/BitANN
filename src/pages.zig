const std = @import("std");
const types = @import("types.zig");

const mem = std.mem;
const testing = std.testing;

const ItemType = types.ItemType;
const Metadata = types.Metadata;

pub const RawPage = []u8;

pub const PageType = enum(u8) {
    metadata,
    collection,
    cluster,
    vector,
};

pub const PageHeader = packed struct {
    page_type: PageType,
    page_num: u32,
};

pub fn PageParams(comptime page_type: PageType) type {
    return switch (page_type) {
        .metadata => Metadata,
        .collection => struct { prev_page: u32 },
        .cluster => struct { prev_page: u32, cluster_size: u32 },
        .vector => struct { prev_page: u32, vector_size: u32 },
    };
}

pub fn Page(comptime page_type: PageType) type {
    return switch (page_type) {
        .metadata => MetadataPage,
        .collection => ItemPage(.collection),
        .cluster => ItemPage(.cluster),
        .vector => ItemPage(.vector),
    };
}

pub fn create_page(comptime page_type: PageType, raw: RawPage, page_num: u32, params: PageParams(page_type)) *Page(page_type) {
    return switch (page_type) {
        .metadata => MetadataPage.create(raw, params),
        .collection => ItemPage(.collection).create(raw, page_num, params.prev_page, @sizeOf(types.StoredCollection)),
        .cluster => ItemPage(.cluster).create(raw, page_num, params.prev_page, params.cluster_size),
        .vector => ItemPage(.vector).create(raw, page_num, params.prev_page, params.vector_size),
    };
}

pub const MetadataPage = struct {
    header: PageHeader,
    meta: Metadata,

    pub fn init(raw: RawPage) !*MetadataPage {
        return @ptrCast(@alignCast(raw));
    }

    pub fn create(raw: RawPage, meta: Metadata) *MetadataPage {
        const page: *MetadataPage = @ptrCast(@alignCast(raw));
        page.header = PageHeader{
            .page_type = .metadata,
            .page_num = 0,
        };
        page.meta = meta;
        return page;
    }
};

pub fn ItemPage(comptime item_type: ItemType) type {
    return struct {
        const IPage = @This();

        const Item = switch (item_type) {
            .collection => types.StoredCollection,
            .cluster => types.StoredCluster,
            .vector => types.StoredVector,
        };

        header: PageHeader,
        prev_page: u32,
        next_page: u32,
        item_slots: u32,
        item_size: u32,
        item_count: u32,
        // Bitmap follows immediately after this header
        // Items follow after the bitmap

        pub fn create(raw: RawPage, page_num: u32, prev_page: u32, item_size: u32) *IPage {
            @memset(raw, 0);
            const page: *IPage = @ptrCast(@alignCast(raw));
            const available_space = raw.len - @sizeOf(IPage);

            page.* = IPage{
                .header = PageHeader{
                    .page_type = switch (item_type) {
                        .collection => PageType.collection,
                        .cluster => PageType.cluster,
                        .vector => PageType.vector,
                    },
                    .page_num = page_num,
                },
                .prev_page = prev_page,
                .next_page = 0,
                .item_slots = @intCast((available_space * 8) / (item_size * 8 + 1)),
                .item_size = item_size,
                .item_count = 0,
            };
            return page;
        }

        pub fn get(page: *const IPage, index: u32) ?Item {
            if (index >= page.item_slots) return null;
            if (page.is_slot_empty(index)) return null;

            const raw = page.get_raw_item(index);
            return Item.deserialize(raw);
        }

        pub fn insert(page: *IPage, index: u32, item: Item) !void {
            if (index >= page.item_slots) return error.IndexOutOfBounds;

            const raw = page.get_raw_item_mut(index);
            try item.serialize(raw);

            page.set_slot_occupied(index);
            page.item_count += 1;
        }

        pub fn remove(page: *IPage, index: u32) void {
            if (index >= page.item_slots) return;
            if (page.is_slot_empty(index)) return;

            page.set_slot_empty(index);
            page.item_count -= 1;
        }

        fn is_slot_empty(page: *const IPage, index: u32) bool {
            const bitmap = page.get_bitmap_ptr();
            const byte_index = index / 8;
            const bit_index = index % 8;
            const mask: u8 = @as(u8, 1) << @intCast(bit_index);
            return (bitmap[byte_index] & mask) == 0;
        }

        fn set_slot_empty(page: *IPage, index: u32) void {
            const bitmap = page.get_bitmap_ptr_mut();
            const byte_index = index / 8;
            const bit_index = index % 8;
            const mask: u8 = @as(u8, 1) << @intCast(bit_index);
            bitmap[byte_index] &= ~mask;
        }

        fn set_slot_occupied(page: *IPage, index: u32) void {
            const bitmap = page.get_bitmap_ptr_mut();
            const byte_index = index / 8;
            const bit_index = index % 8;
            const mask: u8 = @as(u8, 1) << @intCast(bit_index);
            bitmap[byte_index] |= mask;
        }

        pub fn is_full(page: *const IPage) bool {
            return page.item_count == page.item_slots;
        }

        fn get_raw_item(page: *const IPage, index: u32) []const u8 {
            const items_ptr = page.get_items_ptr();
            const offset = index * page.item_size;
            return items_ptr[offset .. offset + page.item_size];
        }

        fn get_raw_item_mut(page: *IPage, index: u32) []u8 {
            const items_ptr = page.get_items_ptr_mut();
            const offset = index * page.item_size;
            return items_ptr[offset .. offset + page.item_size];
        }

        fn get_items_ptr(page: *const IPage) [*]const u8 {
            const page_bytes: [*]const u8 = @ptrCast(page);
            const bitmap_size = (page.item_slots + 7) / 8;
            return page_bytes + @sizeOf(IPage) + bitmap_size;
        }

        fn get_items_ptr_mut(page: *IPage) [*]u8 {
            const page_bytes: [*]u8 = @ptrCast(page);
            const bitmap_size = (page.item_slots + 7) / 8;
            return page_bytes + @sizeOf(IPage) + bitmap_size;
        }

        fn get_bitmap_ptr(page: *const IPage) [*]const u8 {
            const page_bytes: [*]const u8 = @ptrCast(page);
            return page_bytes + @sizeOf(IPage);
        }

        fn get_bitmap_ptr_mut(page: *IPage) [*]u8 {
            const page_bytes: [*]u8 = @ptrCast(page);
            return page_bytes + @sizeOf(IPage);
        }
    };
}
