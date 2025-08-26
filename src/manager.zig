const std = @import("std");
const pages = @import("pages.zig");

const Pager = @import("pager.zig");

const Page = pages.Page;

pub const PageManager = struct {
    pager: *Pager,

    pub fn init(pager: *Pager) PageManager {
        return .{ .pager = pager };
    }

    pub fn remove_page(manager: *PageManager, page_num: u32) !void {
        return manager.pager.free_page(page_num);
    }

    pub fn add_page(manager: *PageManager) !Page {
        return manager.pager.alloc_page();
    }
};
