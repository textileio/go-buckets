package buckets

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"

	ipfsfiles "github.com/ipfs/go-ipfs-files"
	iface "github.com/ipfs/interface-go-ipfs-core"
	ifaceopts "github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/dcrypto"
)

type fileAdder struct {
	reader io.ReadCloser
	writer io.WriteCloser
}

type addedFile struct {
	path     string
	resolved path.Resolved
	size     int64
}

type fileQueue struct {
	q    map[string]*fileAdder
	lock sync.Mutex
}

func newFileQueue() *fileQueue {
	return &fileQueue{q: make(map[string]*fileAdder)}
}

func (q *fileQueue) add(
	ctx context.Context,
	ufs iface.UnixfsAPI,
	pth string,
	addFunc func() ([]byte, error),
	doneCh chan<- addedFile,
	errCh chan<- error,
) (*fileAdder, error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	fa, ok := q.q[pth]
	if ok {
		return fa, nil
	}

	key, err := addFunc()
	if err != nil {
		return nil, err
	}

	reader, writer := io.Pipe()
	fa = &fileAdder{
		reader: reader,
		writer: writer,
	}
	q.q[pth] = fa

	eventCh := make(chan interface{})
	chSize := make(chan string)
	go func() {
		for e := range eventCh {
			event, ok := e.(*iface.AddEvent)
			if !ok {
				log.Error("unexpected event type")
				continue
			}
			if event.Path != nil {
				chSize <- event.Size // Save size for use in the final response
			}
		}
	}()

	var r io.Reader
	if key != nil {
		r, err = dcrypto.NewEncrypter(reader, key)
		if err != nil {
			return nil, fmt.Errorf("creating decrypter: %v", err)
		}
	} else {
		r = reader
	}

	go func() {
		defer close(eventCh)
		res, err := ufs.Add(
			ctx,
			ipfsfiles.NewReaderFile(r),
			ifaceopts.Unixfs.CidVersion(1),
			ifaceopts.Unixfs.Pin(false),
			ifaceopts.Unixfs.Progress(true),
			ifaceopts.Unixfs.Events(eventCh),
		)
		if err != nil {
			errCh <- fmt.Errorf("adding file: %v", err)
			return
		}
		size := <-chSize
		added, err := strconv.Atoi(size)
		if err != nil {
			errCh <- fmt.Errorf("getting file size: %v", err)
			return
		}
		doneCh <- addedFile{path: pth, resolved: res, size: int64(added)}
	}()

	return fa, nil
}
