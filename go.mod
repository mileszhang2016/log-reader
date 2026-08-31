module github.com/bfenetworks/log-reader

go 1.22

toolchain go1.22.9

require (
	github.com/bfenetworks/bfe v1.8.6
	github.com/bfenetworks/bfe-access-pb v0.3.5
	github.com/bfenetworks/go-lib v0.0.3
	github.com/segmentio/kafka-go v0.4.47
	google.golang.org/protobuf v1.36.5
	gopkg.in/gcfg.v1 v1.2.3
)

require (
	github.com/jehiah/go-strftime v0.0.0-20171201141054-1d33003b3869 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	gopkg.in/warnings.v0 v0.1.2 // indirect
)

// replace github.com/bfenetworks/bfe-access-pb => ../bfe-access-pb

// replace github.com/bfenetworks/bfe => ../bfe
