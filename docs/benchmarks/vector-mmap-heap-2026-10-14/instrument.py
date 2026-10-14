from pathlib import Path
import json
root=Path('/Users/cypark/Documents/project/latticedb-go')
out=Path('/tmp/latticedb-mmap-20261014')
s=(root/'vector_disk_benchmark_test.go').read_text().replace('"os"','"os"\n "os/exec"\n "strconv"')
s=s.replace('if os.Getenv("LATTICEDB_VECTOR_BUILD_PROGRESS")', 'if end == len(vectors) || end == 50000 { memorySnapshot(fmt.Sprintf("built-%d", end)) }; if os.Getenv("LATTICEDB_VECTOR_BUILD_PROGRESS")')
s=s.replace('build := time.Since(start)', 'memorySnapshot("closed"); build := time.Since(start)')
s=s.replace('reopen := time.Since(start)', 'reopen := time.Since(start); memorySnapshot("reopened")')
s += '''
func memorySnapshot(phase string) {
 var m runtime.MemStats
 runtime.ReadMemStats(&m)
 fmt.Fprintf(os.Stderr, "SNAPSHOT %s alloc=%d inuse=%d sys=%d idle=%d released=%d objects=%d gc=%d\\n", phase,m.HeapAlloc,m.HeapInuse,m.HeapSys,m.HeapIdle,m.HeapReleased,m.HeapObjects,m.NumGC)
 data,err:=exec.Command("/usr/bin/vmmap","-summary",strconv.Itoa(os.Getpid())).CombinedOutput()
 if err != nil { panic(string(data)) }
 if err:=os.WriteFile(filepath.Join(os.Getenv("SNAPSHOT_DIR"),phase+".vmmap"),data,0600);err!=nil {panic(err)}
}
'''
(out/'instrument.go').write_text(s)
(out/'overlay.json').write_text(json.dumps({'Replace':{str(root/'vector_disk_benchmark_test.go'):str(out/'instrument.go')}}))
