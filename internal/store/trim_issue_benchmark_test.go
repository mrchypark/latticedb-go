package store
import("fmt";"testing")
func BenchmarkRepeatedTrim(b *testing.B) {
 for _, n := range []int{1000,10000,100000} {
  b.Run(fmt.Sprint(n),func(b *testing.B){
   base:=benchmarkStreamStore(n)
   b.ReportAllocs();b.ResetTimer()
   for range b.N {
    fork:=base.Fork()
    for seq:=uint64(1);seq<=100;seq++ {fork.Trim("events",seq)}
   }
  })
 }
}
