const std = @import("std");
const pages = @import("pages.zig");
const types = @import("types.zig");

const Pager = @import("pager.zig").Pager;

const assert = std.debug.assert;
const Page = pages.Page;

const Item = types.Item;
const ItemType = types.ItemType;

pub fn ItemCursor(comptime item_type: ItemType) type {
    return struct {
        const page_type = switch (item_type) {
            .collection => pages.PageType.collection,
            .cluster => pages.PageType.cluster,
            .vector => pages.PageType.vector,
        };

        const Cursor = @This();

        pager: *Pager,
        page: *Page(page_type),
        index: u32 = 0,

        pub fn init(pager: *Pager, first_page_num: u32) !Cursor {
            const page = try pager.get_page(page_type, first_page_num);
            assert(page.header.prev_page == 0);

            return Cursor{
                .pager = pager,
                .page = page,
            };
        }

        pub fn deinit(cursor: *Cursor) void {
            cursor.pager.release_page(cursor.page);
        }

        pub fn next(cursor: *Cursor) !bool {
            if (cursor.index < cursor.page.slots - 1) {
                cursor.index += 1;
                return true;
            }
            if (cursor.page.header.next_page != 0) {
                const old_page = cursor.page;
                cursor.page = try cursor.pager.get_page(page_type, old_page.header.next_page);
                cursor.pager.release_page(old_page);
                cursor.index = 0;
                return true;
            }
            return false;
        }

        pub fn prev(cursor: *Cursor) !bool {
            if (cursor.index > 0) {
                cursor.index -= 1;
                return true;
            }
            if (cursor.page.header.prev_page != 0) {
                const old_page = cursor.page;
                cursor.page = try cursor.pager.get_page(page_type, old_page.header.prev_page);
                cursor.pager.release_page(old_page);
                cursor.index = cursor.page.slots - 1;
                return true;
            }
            return false;
        }

        pub fn seek_to_start(cursor: *Cursor) !void {
            while (cursor.page.header.prev_page != 0) {
                const old_page = cursor.page;
                cursor.page = try cursor.pager.get_page(page_type, old_page.header.prev_page);
                cursor.pager.release_page(old_page);
            }
            cursor.index = 0;
        }

        pub fn seek_to_end(cursor: *Cursor) !void {
            while (cursor.page.header.next_page != 0) {
                const old_page = cursor.page;
                cursor.page = try cursor.pager.get_page(page_type, old_page.header.next_page);
                cursor.pager.release_page(old_page);
            }
            cursor.index = cursor.page.slots - 1;
        }

        pub fn is_at_end(cursor: *Cursor) bool {
            return cursor.page.header.next_page == 0 and cursor.index == cursor.page.slots - 1;
        }

        pub fn get_current(cursor: *Cursor) ?Item(item_type) {
            return cursor.page.item_page().get(cursor.index);
        }

        pub fn is_slot_empty(cursor: *Cursor) bool {
            return cursor.page.item_page().get(cursor.index) == null;
        }

        pub fn insert(cursor: *Cursor, item: Item(item_type)) !void {
            try cursor.page.item_page().insert(cursor.index, item);
        }

        //pub fn update(cursor: *VectorCursor, value: []const u8) !void {}
        //pub fn delete(cursor: *VectorCursor) !void {}

        pub fn next_empty_slot(cursor: *Cursor) !void {
            while (cursor.page.item_page().is_full()) {
                if (cursor.page.header.next_page == 0) return error.NoEmptySlots;

                const old_page = cursor.page;
                cursor.page = try cursor.pager.get_page(page_type, old_page.header.next_page);
                cursor.pager.release_page(old_page);
                cursor.index = 0;
            }

            while (cursor.index < cursor.page.slots) {
                if (cursor.is_slot_empty()) return;
                cursor.index += 1;
            }
            return error.NoEmptySlots;
        }

        pub fn iterator(cursor: *Cursor) Iterator {
            return Iterator{ .cursor = cursor };
        }

        pub const Iterator = struct {
            cursor: *Cursor,
            started: bool = false,

            pub fn next(it: *Iterator) !?Item(item_type) {
                if (!it.started) {
                    it.started = true;
                    return it.cursor.get_current();
                }
                if (try it.cursor.next()) {
                    return it.cursor.get_current();
                }
                return null;
            }
        };
    };
}
