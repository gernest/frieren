package store

import (
	"archive/tar"
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/gernest/rbf"
	"github.com/klauspost/compress/zstd"
	"github.com/oklog/ulid/v2"
)

func (s *Store) BackupHandler(w http.ResponseWriter, r *http.Request) {
	name := ulid.Make().String() + "frieren_backup.tar"
	path := filepath.Join(s.path, name)
	tmp, err := os.Create(path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer tmp.Close()
	defer os.Remove(path)
	err = s.Backup(tmp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.ServeContent(w, r, name, time.Now(), tmp)
}

// Backup creates a tar archive of Store. All files inside the archive are zstd
// compressed.
// Files included are
//   - index : rbf backup file
//   - blobs : badger/v4 backup file for blobs
//   - db : badger/v4 backup file for database
func (s *Store) Backup(w io.Writer) error {
	tw := tar.NewWriter(w)
	o := new(bytes.Buffer)
	zw, err := zstd.NewWriter(o)
	if err != nil {
		return err
	}

	// start with index
	err = s.idx.Backup(zw)
	if err != nil {
		return err
	}
	err = write(o, tw, "index")
	if err != nil {
		return err
	}

	// backup blobs
	o.Reset()
	zw.Reset(o)
	_, err = s.blob.Backup(zw, 0)
	if err != nil {
		return err
	}

	err = write(o, tw, "blobs")
	if err != nil {
		return err
	}

	o.Reset()
	zw.Reset(o)
	_, err = s.db.Backup(zw, 0)
	if err != nil {
		return err
	}
	err = write(o, tw, "db")
	if err != nil {
		return err
	}
	return tw.Close()
}

// Restore restores Store from a tar r on path. r should be obtained from
// calling (*Store)Backup
func Restore(path string, r io.Reader) error {
	tr := tar.NewReader(r)
	zr, err := zstd.NewReader(nil)
	if err != nil {
		return err
	}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		switch hdr.Name {
		case "index":
			// Make sure the directory exists
			dbPath := filepath.Join(path, hdr.Name)
			os.MkdirAll(dbPath, 0o755)
			err = zr.Reset(tr)
			if err != nil {
				return err
			}
			err = rbf.Restore(zr, dbPath)
			if err != nil {
				return err
			}
		case "blobs", "db":
			dbPath := filepath.Join(path, hdr.Name)
			db, err := badger.Open(badger.DefaultOptions(dbPath))
			if err != nil {
				return err
			}
			err = zr.Reset(tr)
			if err != nil {
				db.Close()
				return err
			}
			err = db.Load(zr, runtime.NumCPU())
			if err != nil {
				db.Close()
				return err
			}
			err = db.Close()
			if err != nil {
				return err
			}
		}
	}
}

func write(o *bytes.Buffer, tw *tar.Writer, name string) error {
	err := tw.WriteHeader(&tar.Header{
		Name: name,
		Mode: 0600,
		Size: int64(o.Len()),
	})
	if err != nil {
		return err
	}
	_, err = tw.Write(o.Bytes())
	return err
}
