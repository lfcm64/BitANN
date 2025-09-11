const std = @import("std");
const pages = @import("pages.zig");
const types = @import("types.zig");

const Pager = @import("pager.zig").Pager;

const ItemPage = pages.ItemPage;
const Vector = types.Vector;

const Item = types.Item;
const ItemType = types.ItemType;

pub fn ChainedItemCursor(comptime item_type: ItemType) type {
    return struct {
        const page_type = switch (item_type) {
            .collection => pages.PageType.collection,
            .cluster => pages.PageType.cluster,
            .vector => pages.PageType.vector,
        };

        const Cursor = @This();

        pager: *Pager,
        page: *ItemPage(item_type),
        index: u32 = 0,

        pub fn init(pager: *Pager, first_page_num: u32) !Cursor {
            const page = try pager.get_page(page_type, first_page_num);
            return Cursor{
                .pager = pager,
                .page = page,
            };
        }

        pub fn deinit(cursor: *Cursor) void {
            cursor.pager.release_page(cursor.page);
        }

        pub fn next(cursor: *Cursor) !bool {
            if (cursor.index < cursor.page.item_slots - 1) {
                cursor.index += 1;
                return true;
            }
            if (cursor.page.next_page != 0) {
                const old_page = cursor.page;
                cursor.page = try cursor.pager.get_page(page_type, old_page.next_page);
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
                cursor.page = try cursor.pager.get_page(page_type, old_page.prev_page);
                cursor.pager.release_page(old_page);
                cursor.index = cursor.page.item_slots - 1;
                return true;
            }
            return false;
        }

        pub fn seek_to_start(cursor: *Cursor) !void {
            while (cursor.page.header.prev_page != 0) {
                const old_page = cursor.page;
                cursor.page = try cursor.pager.get_page(page_type, old_page.prev_page);
                cursor.pager.release_page(old_page);
            }
            cursor.index = 0;
        }

        pub fn seek_to_end(cursor: *Cursor) !void {
            while (cursor.page.next_page != 0) {
                const old_page = cursor.page;
                cursor.page = try cursor.pager.get_page(page_type, old_page.next_page);
                cursor.pager.release_page(old_page);
            }
            cursor.index = cursor.page.item_slots - 1;
        }

        pub fn is_at_end(cursor: *Cursor) bool {
            return cursor.page.next_page == 0 and cursor.index == cursor.page.item_slots - 1;
        }

        pub fn get_current(cursor: *Cursor) !?Item(item_type) {
            return cursor.page.get(cursor.index);
        }

        pub fn is_slot_empty(cursor: *Cursor) bool {
            return cursor.page.get(cursor.index) == null;
        }

        pub fn next_empty_slot(cursor: *Cursor) !void {
            while (cursor.page.is_full()) {
                if (cursor.page.next_page == 0) return error.NoEmptySlots;

                const old_page = cursor.page;
                cursor.page = try cursor.pager.get_page(page_type, old_page.next_page);
                cursor.pager.release_page(old_page);
                cursor.index = 0;
            }

            while (cursor.index < cursor.page.item_slots) {
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
