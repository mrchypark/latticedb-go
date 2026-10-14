from pathlib import Path
p=Path('/tmp/latticedb-mmap-20261014')
s=Path('scripts/vector-disk-zig.zig').read_text()
s=s.replace('first = last;', 'first = last;\n            if (first == 50000 or first == fixture.n) snapshot("built");')
s=s.replace('try db.sync();\n        db.close();','try db.sync();\n        db.close();\n        snapshot("closed");')
s=s.replace('reopen_ns = compat.nanoTimestamp() - reopen_start;', 'reopen_ns = compat.nanoTimestamp() - reopen_start;\n        snapshot("reopened");')
s += '''\nextern "c" fn raise(sig: c_int) c_int;\nfn snapshot(phase: []const u8) void { std.debug.print("SNAPSHOT {s}\\n", .{phase}); _ = raise(17); }\n'''
(p/'instrument.zig').write_text(s)
