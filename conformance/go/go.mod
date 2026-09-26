module github.com/mrchypark/latticedb-go/conformance/go

go 1.27

toolchain go1.27.1

require github.com/mrchypark/latticedb-go v0.0.0

require (
	go.etcd.io/bbolt v1.4.3 // indirect
	golang.org/x/sys v0.29.0 // indirect
)

replace github.com/mrchypark/latticedb-go => ../..
