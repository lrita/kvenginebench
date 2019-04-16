package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"time"
	_ "unsafe"

	"github.com/juju/errors"
	"github.com/lrita/cache"
	"github.com/lrita/kvenginebench"
)

func fatalf(f string, v ...interface{}) {
	fmt.Printf(f, v...)
	fmt.Println()
	os.Exit(-1)
}

var (
	gencache cache.BufCache
)

func genbytes(length int) []byte {
	b := gencache.Get()
	if len(b) < length {
		b = make([]byte, length)
	}
	for i := 0; i < length; i++ {
		b[i] = byte(fastrandn(256))
	}
	return b
}

func cleanall() {
	if runtime.GOOS == "linux" {
		ioutil.WriteFile("/proc/sys/vm/drop_caches", []byte{'3'}, 0644)
	}
	runtime.GC()
	runtime.GC()
}

func main() {
	var (
		klen     = flag.Int("key_len", 24, "key length")
		vlen     = flag.Int("val_len", 24, "value length")
		loadSize = flag.Int64("load_size", 1, "load testing size, GB")
		fsync    = flag.Bool("fsync", false, "using fsync")
		base     = flag.String("base_path", "./tmp", "base directory")

		engines = []struct {
			name    string
			factory func(path string, fsync bool) (kvenginebench.Engine, error)
		}{
			{
				"goleveldb",
				kvenginebench.NewGoLevelDB,
			},
			{
				"badger",
				kvenginebench.NewBadgerDB,
			},
			{
				"gorocksdb",
				kvenginebench.NewRocksDB,
			},
		}
	)

	flag.Parse()

	*loadSize <<= 30

	for _, e := range engines {
		datapath := filepath.Join(*base, e.name)
		os.RemoveAll(datapath)
		testing := func(fsync bool) error {
			engine, err := e.factory(datapath, fsync)
			if err != nil {
				return errors.Trace(err)
			}

			var (
				remaining = *loadSize
				one       = *loadSize / 3 * 2
				two       = *loadSize / 3
				stage     int

				begin0 time.Time
				begin1 time.Time
				begin2 time.Time
				begin3 time.Time
				ntime0 int64
				ntime1 int64
				ntime2 int64
			)

			begin0 = time.Now()
			for remaining > 0 {
				key := genbytes(*klen)
				val := genbytes(*vlen)

				if err := engine.Set(key, val); err != nil {
					return fmt.Errorf("set failed: %v", err)
				}
				remaining -= int64(len(key))
				remaining -= int64(len(val))
				gencache.Put(key)
				gencache.Put(val)

				switch stage {
				case 0:
					ntime0++
					if remaining < one {
						begin1 = time.Now()
						stage++
					}
				case 1:
					ntime1++
					if remaining < two {
						begin2 = time.Now()
						stage++
					}
				case 2:
					ntime2++
				}
			}
			begin3 = time.Now()

			fmt.Printf("%v fsync(%v) set testing:\n",
				e.name, fsync)
			fmt.Printf("stage0 %s/op\n", begin1.Sub(begin0)/time.Duration(ntime0))
			fmt.Printf("stage1 %s/op\n", begin2.Sub(begin1)/time.Duration(ntime1))
			fmt.Printf("stage2 %s/op\n", begin3.Sub(begin2)/time.Duration(ntime2))

			cleanall()

			remaining = *loadSize
			stage, ntime0, ntime1, ntime2 = 0, 0, 0, 0
			begin0 = time.Now()
			engine.Each(func(key, val []byte) bool {
				switch stage {
				case 0:
					ntime0++
					if remaining < one {
						begin1 = time.Now()
						stage++
					}
				case 1:
					ntime1++
					if remaining < two {
						begin2 = time.Now()
						stage++
					}
				case 2:
					ntime2++
				}

				remaining -= int64(len(key))
				remaining -= int64(len(val))
				return true
			})
			begin3 = time.Now()

			fmt.Printf("%v fsync(%v) foreach testing:\n",
				e.name, fsync)
			fmt.Printf("stage0 %s/op\n", begin1.Sub(begin0)/time.Duration(ntime0))
			fmt.Printf("stage1 %s/op\n", begin2.Sub(begin1)/time.Duration(ntime1))
			fmt.Printf("stage2 %s/op\n", begin3.Sub(begin2)/time.Duration(ntime2))

			return nil
		}
		if err := testing(*fsync); err != nil {
			fatalf("test %v fsync(%v) failed: %v", e.name, *fsync, err)
		}
		cleanall()
	}
}

//go:linkname fastrandn runtime.fastrandn
func fastrandn(n uint32) uint32
