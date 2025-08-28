const std = @import("std");

const Db = @import("db.zig");

const SUCCESS: c_int = 0;
const ERROR_INVALID_PATH: c_int = -1;
const ERROR_FILE_ACCESS: c_int = -2;
const ERROR_MEMORY: c_int = -3;
const ERROR_CORRUPT_DB: c_int = -4;
const ERROR_INVALID_HANDLE: c_int = -5;
const ERROR_UNKNOWN: c_int = -6;

export fn open(file_path_ptr: [*c]const u8, file_path_len: c_int, db_handle: *?*anyopaque) c_int {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const file_path = file_path_ptr[0..@intCast(file_path_len)];
    db_handle.* = Db.init(allocator, file_path) catch return ERROR_UNKNOWN;
    return SUCCESS;
}

//export fn close(ptr: *anyopaque) c_int {}

//export fn create_collection() void {}
//export fn open_collection() void {}
//export fn close_collection() void {}

//export fn search_vector() void {}
//export fn add_vector() void {}
