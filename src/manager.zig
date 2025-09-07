const std = @import("std");
const pages = @import("pages.zig");
const types = @import("types.zig");
const cursors = @import("cursors.zig");

const Pager = @import("pager.zig").Pager;

const assert = std.debug.assert;
const Page = pages.Page;

const Item = types.Item;
const ItemType = types.ItemType;

pub fn ItemManager(comptime item_type: ItemType) type {
    return struct {
        const page_type = switch (item_type) {
            .collection => pages.PageType.collection,
            .cluster => pages.PageType.cluster,
            .vector => pages.PageType.vector,
        };

        const Manager = @This();

        pager: *Pager,
        cursor: cursors.ItemCursor(item_type),

        pub fn init(pager: *Pager, initial_page_num: u32) !Manager {
            const cursor = try cursors.ItemCursor(item_type).init(pager, initial_page_num);
            return Manager{
                .pager = pager,
                .cursor = cursor,
            };
        }

        pub fn deinit(manager: *Manager) void {
            manager.cursor.deinit();
        }

        pub fn append(manager: *Manager, item: Item(item_type)) !void {
            manager.cursor.next_empty_slot() catch {
                try manager.cursor.seek_to_end();
                const old_last_page = manager.cursor.page;

                const params = switch (item_type) {
                    .vector => pages.PageParams(.vector){ .prev_page = old_last_page.header.page_num, .quantization = old_last_page.quantization },
                    .cluster => pages.PageParams(.cluster){ .prev_page = old_last_page.header.page_num, .quantization = old_last_page.quantization },
                    .collection => pages.PageParams(.collection){ .prev_page = old_last_page.header.page_num },
                };

                const new_last_page = try manager.pager.new_page(page_type, params);
                old_last_page.header.next_page = new_last_page.header.page_num;
                try manager.cursor.next_empty_slot();
            };
            try manager.cursor.insert(item);
        }
    };
}
