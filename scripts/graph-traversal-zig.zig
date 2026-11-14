//! Disk-backed graph traversal benchmark for the Zig LatticeDB API.
//!
//! Run from /tmp/latticedb-zig-v017 with the command in the task report.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const QueryResult = lattice.storage.database.QueryResult;
const node_count: usize = 10_000;
const smoke_node_count: usize = 8;
const repeats = 3;

const one_hop = "MATCH (a)-[:NEXT]->(b) RETURN id(b) AS id";
const two_hop = "MATCH (a)-[:NEXT]->(b)-[:NEXT]->(c) RETURN id(c) AS id";

pub fn main(init: std.process.Init.Minimal) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    var args = std.process.Args.Iterator.init(init.args);
    _ = args.next();
    const db_path = args.next() orelse return error.InvalidArguments;
    const count = if (args.next()) |arg|
        if (std.mem.eql(u8, arg, "--smoke") and args.next() == null) smoke_node_count else return error.InvalidArguments
    else
        node_count;

    try requireMissingPath(db_path);
    try createChain(allocator, db_path, count);

    var db = try Database.open(allocator, db_path, .{
        .config = .{ .enable_fts = false, .enable_vector = false },
    });
    defer db.close();

    const one = try warmAndTime(db, one_hop, count - 1);
    const two = try warmAndTime(db, two_hop, count - 2);

    std.debug.print("nodes={d} persisted_path={s}\n", .{ count, db_path });
    std.debug.print("verified: 1-hop rows={d} unique_ids={d}; 2-hop rows={d} unique_ids={d}\n", .{ count - 1, count - 1, count - 2, count - 2 });
    for (one, 1..) |elapsed_ns, repeat_number| std.debug.print("1-hop repeat={d} elapsed_ns={d}\n", .{ repeat_number, elapsed_ns });
    for (two, 1..) |elapsed_ns, repeat_number| std.debug.print("2-hop repeat={d} elapsed_ns={d}\n", .{ repeat_number, elapsed_ns });
}

fn createChain(allocator: std.mem.Allocator, db_path: []const u8, count: usize) !void {
    var db = try Database.open(allocator, db_path, .{
        .create = true,
        .config = .{ .enable_fts = false, .enable_vector = false },
    });
    defer db.close();

    const ids = try allocator.alloc(u64, count);
    defer allocator.free(ids);

    var txn = try db.beginTransaction(.read_write);
    for (ids, 0..) |*id, index| {
        id.* = try db.createNode(&txn, &.{});
        if (index != 0) try db.createEdge(&txn, ids[index - 1], id.*, "NEXT");
    }
    try db.commitTransaction(&txn);
    try db.sync();
}

fn warmAndTime(db: *Database, query: []const u8, expected_rows: usize) ![repeats]u64 {
    var warmup = try db.query(query);
    defer warmup.deinit();
    try verifyIds(&warmup, expected_rows);

    var elapsed: [repeats]u64 = undefined;
    for (&elapsed) |*entry| {
        {
            const start_ns = @import("compat").nanoTimestamp();
            var result = try db.query(query);
            entry.* = @intCast(@import("compat").nanoTimestamp() - start_ns);
            defer result.deinit();
            try verifyIds(&result, expected_rows);
        }
    }
    return elapsed;
}

fn verifyIds(result: *const QueryResult, expected_rows: usize) !void {
    if (result.columnCount() != 1 or !std.mem.eql(u8, result.columns[0], "id")) return error.UnexpectedColumns;
    if (result.rowCount() != expected_rows) return error.UnexpectedRowCount;

    var ids = std.AutoHashMap(i64, void).init(result.allocator);
    defer ids.deinit();
    for (result.rows) |row| {
        if (row.values.len != 1) return error.UnexpectedColumns;
        const id = switch (row.values[0]) {
            .int_val => |value| value,
            else => return error.UnexpectedIdType,
        };
        if (ids.contains(id)) return error.DuplicateId;
        try ids.put(id, {});
    }
    if (ids.count() != expected_rows) return error.UnexpectedUniqueIdCount;
}

fn requireMissingPath(path: []const u8) !void {
    @import("compat").fs.cwd().access(path, .{}) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    return error.PathAlreadyExists;
}
