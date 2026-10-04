// Adapter for jeffhajewski/latticedb at 827891e. Uses the unchanged FtsIndex.
const std = @import("std");
const lattice = @import("lattice");
const compat = @import("compat");
const a = std.heap.c_allocator;
const Record = struct { id: u64, text: []const u8 };
fn load(path: []const u8) ![]Record {
    const file = try compat.fs.cwd().openFile(path, .{});
    defer file.close();
    const data = try file.readToEndAlloc(a, 128 * 1024 * 1024);
    defer a.free(data);
    var rows: std.ArrayList(Record) = .empty;
    var lines = std.mem.splitScalar(u8, data, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        const tab = std.mem.indexOfScalar(u8, line, '\t') orelse return error.BadInput;
        const id = try std.fmt.parseInt(u64, line[0..tab], 10);
        if (id != rows.items.len + 1 or line.len == tab + 1) return error.BadInput;
        try rows.append(a, .{ .id = id, .text = try a.dupe(u8, line[tab + 1 ..]) });
    }
    if (rows.items.len == 0) return error.EmptyInput;
    return rows.toOwnedSlice(a);
}
fn release(rows: []Record) void {
    for (rows) |r| a.free(r.text);
    a.free(rows);
}
pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(a);
    defer a.free(args);
    if (args.len != 5) return error.UsageCorpusQueriesNewDBRepeats;
    const docs = try load(args[1]);
    defer release(docs);
    const queries = try load(args[2]);
    defer release(queries);
    const repeats = try std.fmt.parseInt(usize, args[4], 10);
    if (repeats == 0) return error.InvalidRepeats;
    if (compat.fs.cwd().openFile(args[3], .{})) |existing| {
        existing.close();
        return error.DatabaseAlreadyExists;
    } else |err| {
        if (err != error.FileNotFound) return err;
    }
    var vfs = lattice.storage.vfs.PosixVfs.init(a);
    const build_start = compat.nanoTimestamp();
    var pm = try lattice.PageManager.init(a, vfs.vfs(), args[3], .{ .create = true });
    var bp = try lattice.BufferPool.init(a, &pm, 64 * 1024 * 1024);
    var dict = try lattice.BTree.init(a, &bp);
    var lengths = try lattice.BTree.init(a, &bp);
    const config: lattice.FtsConfig = .{ .tokenizer = .{ .min_token_length = 1, .max_token_length = 64, .remove_stop_words = false, .use_stemming = false, .remove_accents = false }, .bm25 = .{ .k1 = 1.2, .b = 0.75 } };
    var index = lattice.FtsIndex.init(a, &bp, &dict, &lengths, null, config);
    var total_tokens: u64 = 0;
    for (docs) |doc| {
        const count = try index.indexDocument(doc.id, doc.text);
        if (count == 0) return error.EmptyDocument;
        total_tokens += count;
    }
    const dict_root = dict.getRootPage();
    const lengths_root = lengths.getRootPage();
    try bp.close();
    bp.deinit();
    pm.deinit();
    const build_ns = compat.nanoTimestamp() - build_start;
    const reopen_start = compat.nanoTimestamp();
    pm = try lattice.PageManager.init(a, vfs.vfs(), args[3], .{});
    defer pm.deinit();
    bp = try lattice.BufferPool.init(a, &pm, 64 * 1024 * 1024);
    defer bp.deinit();
    dict = lattice.BTree.open(a, &bp, dict_root);
    lengths = lattice.BTree.open(a, &bp, lengths_root);
    index = lattice.FtsIndex.init(a, &bp, &dict, &lengths, null, config);
    const stats = index.getStats();
    if (stats.total_docs != docs.len or stats.total_tokens != total_tokens) return error.ReopenCountMismatch;
    std.debug.print("meta\tdocuments\t{d}\nmeta\ttokens\t{d}\nmeta\tbuild_ns\t{d}\nmeta\treopen_ns\t{d}\n", .{ stats.total_docs, stats.total_tokens, build_ns, compat.nanoTimestamp() - reopen_start });
    for (queries) |q| {
        const found = try index.searchOr(q.text, 10);
        index.freeResults(found);
    }
    for (0..repeats) |rep| {
        for (queries) |q| {
            const started = compat.nanoTimestamp();
            const found = try index.searchOr(q.text, 10);
            const elapsed = compat.nanoTimestamp() - started;
            defer index.freeResults(found);
            std.debug.print("time\t{d}\t{d}\t{d}\n", .{ rep, q.id, elapsed });
            for (found, 0..) |h, rank| std.debug.print("hit\t{d}\t{d}\t{d}\t{d}\t{d}\n", .{ rep, q.id, rank + 1, h.doc_id, h.score });
        }
    }
}
