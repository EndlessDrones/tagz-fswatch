package main

import (
	"crypto/sha256"
	"github.com/fsnotify/fsnotify"
	"github.com/gabriel-vasile/mimetype"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

const (
	TAGZ_IN_DIR  = "/home/mat/Projects/tagz/tmp/in"
	TAGZ_TMP_DIR = "/home/mat/Projects/tagz/tmp/tmp"
	TAGZ_OUT_DIR = "/home/mat/Projects/tagz/tmp/out"
)

type NewFile struct {
	name    string
	ext     string
	size    int64
	modTime time.Time
	mime    string
	sha256  []byte
}

func processInotify(inotifyWatcher *fsnotify.Watcher, notif chan<- string) {
	for {
		select {
		case event, ok := <-inotifyWatcher.Events:
			if !ok {
				close(notif)
				return
			}
			if event.Op&fsnotify.Write == fsnotify.Write {
				notif <- event.Name
			}
			if event.Op&fsnotify.Create == fsnotify.Create {
				notif <- event.Name
			}
		case err, ok := <-inotifyWatcher.Errors:
			if !ok {
				close(notif)
				return
			}
			log.Println("error:", err)
		}
	}
}

func checkIfMovable(newFile <-chan string, movableFile chan<- string) {
	for filePath := range newFile {
		tgtPath := filepath.Join(TAGZ_TMP_DIR, filepath.Base(filePath))
		if _, err := os.Stat(tgtPath); err != nil {
			err := os.Rename(filePath, tgtPath)
			if err == nil {
				movableFile <- tgtPath
			}
		}
	}
	close(movableFile)
}

func makeWatcher(watchDir string) *fsnotify.Watcher {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}

	err = watcher.Add(watchDir)
	if err != nil {
		log.Fatal(err)
	}

	return watcher
}

func handleMovableFile(movable <-chan string) {
	for filePath := range movable {
		filePathInfo, err := os.Stat(filePath)
		if err != nil {
			log.Panicf("Error on opening file %s : %s", filePath, err)
		}
		if filePathInfo.IsDir() {
			log.Printf("tagz does not support directories (yet): %s", filePath)
		} else {
			log.Printf("Moved file %s", filePath)

			newFile, err := os.Open(filePath)
			if err != nil {
				log.Fatal(err)
			}
			defer newFile.Close()

			buf := make([]byte, 1024*1024)
			h := sha256.New()
			if _, err := io.CopyBuffer(h, newFile, buf); err != nil {
				log.Fatal(err)
			}

			sha256Sum := h.Sum(nil)
			_, err = newFile.Seek(0, 0)
			if err != nil {
				log.Fatal(err)
			}

			mimeType, ext, err := mimetype.DetectReader(newFile)

			if err != nil {
				mimeType = ""
			}
			n := NewFile{name: filePathInfo.Name(), ext: ext, mime: mimeType, modTime: filePathInfo.ModTime(), size: filePathInfo.Size(), sha256: sha256Sum}
			log.Printf("New file: %s %d %s %s %x", n.name, n.size, n.modTime, n.mime, n.sha256)

		}
	}
}

func main() {
	log.Printf("Hello, world! Watching: %s and moving to %s", TAGZ_IN_DIR, TAGZ_TMP_DIR)
	fileWrites := make(chan string)
	fileNotWritingTo := make(chan string)
	watcher := makeWatcher(TAGZ_IN_DIR)
	defer watcher.Close()

	go processInotify(watcher, fileWrites)
	go checkIfMovable(fileWrites, fileNotWritingTo)
	handleMovableFile(fileNotWritingTo)
}
