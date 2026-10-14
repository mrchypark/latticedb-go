// Public disk-vector adapter for jeffhajewski/latticedb at f8e7783 and v0.15.0.
const std = @import("std");
const lattice = @import("lattice");
const compat = @import("compat");

const a = std.heap.smp_allocator;
const dimensions = 128;
const query_count = 100;
const warmup_count = 10;
const recall_query_count = 10;
const k = 10;
const batch_size = 1000;

const Fixture = struct {
    n: usize,
    values: []f32,

    fn deinit(self: *Fixture) void {
        a.free(self.values);
    }

    fn vector(self: *const Fixture, index: usize) []const f32 {
        const start = index * dimensions;
        return self.values[start..][0..dimensions];
    }

    fn query(self: *const Fixture, index: usize) []const f32 {
        const start = self.n * dimensions + index * dimensions;
        return self.values[start..][0..dimensions];
    }
};

const Neighbor = struct {
    id: u64,
    distance: f64,
};

fn lessNeighbor(_: void, lhs: Neighbor, rhs: Neighbor) bool {
    return lhs.distance < rhs.distance or (lhs.distance == rhs.distance and lhs.id < rhs.id);
}

// Match Go's exact L2 oracle; native cosine ANN has the same ordering on unit vectors.
fn squaredL2(left: []const f32, right: []const f32) f64 {
    var sums = [_]f64{0} ** 4;
    for (left, right, 0..) |lhs, rhs, index| {
        const diff = @as(f64, lhs) - @as(f64, rhs);
        sums[index % 4] += diff * diff;
    }
    return (sums[0] + sums[1]) + (sums[2] + sums[3]);
}

fn loadFixture(path: []const u8) !Fixture {
    const file = try compat.fs.cwd().openFile(path, .{});
    defer file.close();
    const stat = try file.stat();
    if (stat.size > 12 + (100000 + query_count) * dimensions * @sizeOf(f32)) return error.BadInput;
    const max_bytes = std.math.add(usize, @intCast(stat.size), 1) catch return error.BadInput;
    const bytes = try file.readToEndAlloc(a, max_bytes);
    defer a.free(bytes);
    if (bytes.len < 12) return error.BadInput;

    const n: usize = std.mem.readInt(u32, bytes[0..4], .little);
    const d = std.mem.readInt(u32, bytes[4..8], .little);
    const q = std.mem.readInt(u32, bytes[8..12], .little);
    if (n == 0 or d != dimensions or q != query_count) return error.BadInput;

    const vector_values = std.math.mul(usize, n, dimensions) catch return error.BadInput;
    const value_count = std.math.add(usize, vector_values, query_count * dimensions) catch return error.BadInput;
    const value_bytes = std.math.mul(usize, value_count, @sizeOf(f32)) catch return error.BadInput;
    const expected = std.math.add(usize, 12, value_bytes) catch return error.BadInput;
    if (bytes.len != expected) return error.BadInput;

    const values = try a.alloc(f32, value_count);
    errdefer a.free(values);
    for (values, 0..) |*value, index| {
        const offset = 12 + index * @sizeOf(f32);
        value.* = @bitCast(std.mem.readInt(u32, bytes[offset..][0..4], .little));
        if (!std.math.isFinite(value.*)) return error.BadInput;
    }
    return .{ .n = n, .values = values };
}

fn ensureAbsent(path: []const u8) !void {
    if (compat.fs.cwd().openFile(path, .{})) |file| {
        file.close();
        return error.DatabaseAlreadyExists;
    } else |err| {
        if (err != error.FileNotFound) return err;
    }
}

fn config() lattice.storage.database.DatabaseConfig {
    return .{
        .buffer_pool_size = 64 * 1024 * 1024,
        .enable_wal = true,
        .enable_fts = false,
        .enable_vector = true,
        .vector_dimensions = dimensions,
    };
}

fn verifyIndexConfig(db: *lattice.storage.database.Database) !void {
    const index = db.hnsw_index orelse return error.MissingHNSWIndex;
    if (index.config.dimensions != dimensions or index.config.m != 16 or
        index.config.m_max0 != 32 or index.config.ef_construction != 200 or
        index.config.metric != .cosine) return error.HNSWConfigMismatch;
}

fn insertBatch(db: *lattice.storage.database.Database, fixture: *const Fixture, first: usize, last: usize) !void {
    var txn = try db.beginTransaction(.read_write);
    var committed = false;
    errdefer if (!committed) db.abortTransaction(&txn) catch {};
    const labels = [_][]const u8{"Vector"};
    for (first..last) |index| {
        const node_id = try db.createNode(&txn, &labels);
        const vector = fixture.vector(index);
        try db.setNodeProperty(&txn, node_id, "vector", .{ .vector_val = vector });
        try db.setNodeVectorInTxn(&txn, node_id, vector);
    }
    try db.commitTransaction(&txn);
    committed = true;
}

fn sameVector(got: []const f32, want: []const f32) bool {
    if (got.len != want.len) return false;
    for (got, want) |actual, expected| {
        if (@as(u32, @bitCast(actual)) != @as(u32, @bitCast(expected))) return false;
    }
    return true;
}

fn verifyEndpoint(db: *lattice.storage.database.Database, node_id: u64, expected: []const f32) !void {
    const vector = try db.getNodeVectorInTxn(null, node_id) orelse return error.ReopenVectorMissing;
    defer a.free(vector);
    var normalized: [dimensions]f32 = undefined;
    @memcpy(&normalized, expected);
    // Cosine HNSW persists its normalized vector; the property preserves raw bits.
    lattice.vector.distance.normalize(&normalized);
    if (!sameVector(vector, &normalized)) return error.ReopenVectorMismatch;

    var property = try db.getNodeProperty(node_id, "vector") orelse return error.ReopenPropertyMissing;
    defer property.deinit(a);
    switch (property) {
        .vector_val => |value| if (!sameVector(value, expected)) return error.ReopenPropertyMismatch,
        else => return error.ReopenPropertyMismatch,
    }
}

fn validateResults(db: *lattice.storage.database.Database, query: []const f32, n: usize) !void {
    const results = try db.vectorSearch(query, k, 64);
    defer db.freeVectorSearchResults(results);
    if (results.len != @min(k, n)) return error.SearchCountMismatch;
    for (results, 0..) |result, index| {
        if (result.node_id == 0 or result.node_id > n or !std.math.isFinite(result.distance)) return error.InvalidSearchResult;
        for (results[0..index]) |previous| {
            if (previous.node_id == result.node_id) return error.DuplicateSearchResult;
        }
    }
}

fn recallForQuery(db: *lattice.storage.database.Database, fixture: *const Fixture, query: []const f32) !usize {
    const exact = try a.alloc(Neighbor, fixture.n);
    defer a.free(exact);
    for (exact, 0..) |*neighbor, index| {
        neighbor.* = .{
            .id = @intCast(index + 1),
            .distance = squaredL2(query, fixture.vector(index)),
        };
    }
    std.mem.sort(Neighbor, exact, {}, lessNeighbor);

    const results = try db.vectorSearch(query, k, 64);
    defer db.freeVectorSearchResults(results);
    const limit = @min(k, fixture.n);
    if (results.len != limit) return error.SearchCountMismatch;
    var hits: usize = 0;
    for (exact[0..limit]) |expected| {
        for (results) |result| {
            if (result.node_id == expected.id) {
                hits += 1;
                break;
            }
        }
    }
    return hits;
}

fn fileBytes(path: []const u8) !u64 {
    const file = try compat.fs.cwd().openFile(path, .{});
    defer file.close();
    return (try file.stat()).size;
}

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(a);
    defer a.free(args);
    if (args.len != 3) return error.UsageInputFileNewDbPath;

    var fixture = try loadFixture(args[1]);
    defer fixture.deinit();
    try ensureAbsent(args[2]);
    const wal_path = try std.fmt.allocPrint(a, "{s}-wal", .{args[2]});
    defer a.free(wal_path);
    try ensureAbsent(wal_path);

    var build_ns: i128 = 0;
    {
        const build_start = compat.nanoTimestamp();
        var db = try lattice.storage.database.Database.open(a, args[2], .{ .create = true, .config = config() });
        var closed = false;
        defer if (!closed) db.close();
        try verifyIndexConfig(db);
        var first: usize = 0;
        while (first < fixture.n) {
            const last = @min(first + batch_size, fixture.n);
            try insertBatch(db, &fixture, first, last);
            first = last;
        }
        try db.sync();
        db.close();
        closed = true;
        build_ns = compat.nanoTimestamp() - build_start;
    }
    const bytes = try fileBytes(args[2]);

    var reopen_ns: i128 = 0;
    var timings: [query_count]u64 = undefined;
    var recall_hits: usize = 0;
    {
        const reopen_start = compat.nanoTimestamp();
        var db = try lattice.storage.database.Database.open(a, args[2], .{ .config = config() });
        defer db.close();
        reopen_ns = compat.nanoTimestamp() - reopen_start;
        try verifyIndexConfig(db);
        if (db.nodeCount() != fixture.n or !db.hasPersistedVectorIndex()) return error.ReopenCountMismatch;
        try verifyEndpoint(db, 1, fixture.vector(0));
        try verifyEndpoint(db, @intCast(fixture.n), fixture.vector(fixture.n - 1));

        for (0..warmup_count) |index| try validateResults(db, fixture.query(index), fixture.n);
        for (0..query_count) |index| {
            const started = compat.nanoTimestamp();
            const results = try db.vectorSearch(fixture.query(index), k, 64);
            db.freeVectorSearchResults(results);
            timings[index] = @intCast(compat.nanoTimestamp() - started);
        }
        for (0..query_count) |index| try validateResults(db, fixture.query(index), fixture.n);

        for (0..recall_query_count) |index| recall_hits += try recallForQuery(db, &fixture, fixture.query(index));
        try db.sync();
    }
    const recall_denominator = recall_query_count * @as(usize, @min(k, fixture.n));
    if (recall_hits > recall_denominator) return error.InvalidRecall;
    const recall = @as(f64, @floatFromInt(recall_hits)) * 100.0 / @as(f64, @floatFromInt(recall_denominator));

    std.mem.sort(u64, timings[0..], {}, std.sort.asc(u64));
    var total: u128 = 0;
    for (timings) |elapsed| total += elapsed;
    const mean_ns = total / query_count;
    const p99_ns = timings[(query_count - 1) * 99 / 100];
    std.debug.print("DISK_VECTOR_V1 n={d} build_ms={d} reopen_ms={d} mean_ns={d} p99_ns={d} recall={d:.2} file_bytes={d}\n", .{
        fixture.n,
        @divTrunc(build_ns, std.time.ns_per_ms),
        @divTrunc(reopen_ns, std.time.ns_per_ms),
        mean_ns,
        p99_ns,
        recall,
        bytes,
    });
}
