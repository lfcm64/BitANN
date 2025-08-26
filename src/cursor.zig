const std = @import("std");
const pages = @import("pages.zig");

const Pager = @import("pager.zig");

const Page = pages.Page;

pub const PageCursor = struct {
    pager: *Pager,
    current: ?*Page = null,

    pub fn init(pager: *Pager) PageCursor {
        return .{ .pager = pager };
    }

    pub fn seek(cursor: *PageCursor, page_num: u32) !void {
        cursor.current = try cursor.pager.get_page(page_num);
    }

    pub fn has_next(cursor: *PageCursor) bool {
        if (cursor.current) |current| {
            const header = pages.page_header(current.page);
            return header.next_page != 0;
        }
        return false;
    }

    pub fn next(cursor: *PageCursor) !void {
        if (cursor.current) |current| {
            const header = pages.page_header(current.page);
            const next_page_num = header.next_page;
            if (next_page_num != 0) {
                cursor.current = try cursor.pager.get_page(next_page_num);
                return;
            }
            return error.EndOfPages;
        }
        cursor.current = try cursor.pager.get_page(0);
    }

    pub fn current_page(cursor: *PageCursor) !*Page {
        if (cursor.current) |current| {
            return current;
        }
        return error.NoPage;
    }

    pub fn reset(cursor: *PageCursor) !void {
        cursor.current = null;
    }
};
