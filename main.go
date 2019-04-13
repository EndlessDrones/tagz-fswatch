package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/gabriel-vasile/mimetype"
	"io"
	"io/ioutil"
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

type FileMeta struct {
	origName  string
	origExt   string
	sizeB     int64
	modTime   time.Time
	mime      string
	mimeExt   string
	sha256    []byte
	sha256Str string
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

func checkIfMovable(tmpDir string, newFile <-chan string, movableFile chan<- string) {
	for filePath := range newFile {
		tgtPath := filepath.Join(tmpDir, filepath.Base(filePath))
		if _, err := os.Stat(tgtPath); err != nil {
			err := os.Rename(filePath, tgtPath)
			if err == nil {
				movableFile <- tgtPath
			}
		}
	}
	close(movableFile)
}

func handleMovableFiles(tmpDir string, outDir string, movedFilePaths <-chan string, fileMetas chan<- FileMeta) {
	for filePath := range movedFilePaths {
		go func() {
			fileMeta, err := getFileMeta(filePath)
			if err != nil {
				log.Print(err)
			} else {
				err := moveFromTmpDoTgt(tmpDir, outDir, fileMeta)
				if err != nil {
					log.Print(err)
				} else {
					fileMetas <- fileMeta
				}
			}
		}()
	}
}

func moveFromTmpDoTgt(fromPath string, toPath string, file FileMeta) error {
	tmpPath := filepath.Join(fromPath, file.origName)
	tgtPath := buildTgtFilePath(toPath, file)
	if _, err := os.Stat(tgtPath); err != nil {
		err := os.Rename(tmpPath, tgtPath)
		if err != nil {
			return errors.New(fmt.Sprintf("Error on moving file %s from %s to %s: %s", file.origName, fromPath, toPath, err))
		} else {
			return nil
		}
	} else {
		err := os.Remove(tmpPath)
		if err != nil {
			return errors.New(fmt.Sprintf("Couldn't remove ignored file %s", tmpPath))
		} else {
			return errors.New(fmt.Sprintf("Ignoring %s as there's file with same hash under %s", tmpPath, toPath))
		}
	}
}

func buildTgtFilePath(outDir string, fileMeta FileMeta) string {
	if fileMeta.origExt != "" {
		return filepath.Join(outDir, fileMeta.sha256Str+fileMeta.origExt)
	} else {
		return filepath.Join(outDir, fileMeta.sha256Str)
	}
}

func getFileMeta(filePath string) (FileMeta, error) {
	fileStatInfo, err := os.Stat(filePath)
	if err != nil {
		return FileMeta{}, errors.New(fmt.Sprintf("Error on opening file %s : %s", filePath, err))
	}
	if fileStatInfo.IsDir() {
		return FileMeta{}, errors.New(fmt.Sprintf("tagz does not support directories (yet): %s", filePath))
	} else {
		newFile, err := os.Open(filePath)
		if err != nil {
			return FileMeta{}, errors.New(fmt.Sprintf("Error on opening file %s : %s", filePath, err))
		}
		defer newFile.Close()

		buf := make([]byte, 1024*1024)
		h := sha256.New()
		if _, err := io.CopyBuffer(h, newFile, buf); err != nil {
			return FileMeta{}, errors.New(fmt.Sprintf("Error on calculating file hash %s : %s", filePath, err))
		}

		sha256Sum := h.Sum(nil)
		_, err = newFile.Seek(0, 0)
		if err != nil {
			return FileMeta{}, errors.New(fmt.Sprintf("Error on seeking to the beginning of the file %s : %s", filePath, err))
		}

		mimeType, mimeExt, err := mimetype.DetectReader(newFile)
		ext := filepath.Ext(filePath)

		if err != nil {
			return FileMeta{}, errors.New(fmt.Sprintf("Error on determining file %s MIME type: %s", filePath, err))
		}
		n := FileMeta{origName: fileStatInfo.Name(), origExt: ext, mime: mimeType, mimeExt: mimeExt, modTime: fileStatInfo.ModTime().UTC(), sizeB: fileStatInfo.Size(), sha256: sha256Sum, sha256Str: fmt.Sprintf("%x", sha256Sum)}
		return n, nil
	}
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

func getFilePaths(tmpDir string) []string {
	files, err := ioutil.ReadDir(tmpDir)
	if err != nil {
		return []string{}
	} else {
		var paths []string
		for _, fi := range files {
			paths = append(paths, filepath.Join(tmpDir, fi.Name()))
		}
		return paths
	}
}

func watchForNewFiles(watcher *fsnotify.Watcher, tmpDir string, outDir string, newFileMeta chan<- FileMeta) {
	writesFromInotify := make(chan string)
	movableFiles := make(chan string)

	tmpFiles := getFilePaths(tmpDir)
	go func() {
		log.Printf("Handling initially %d files from directory %s", len(tmpFiles), tmpDir)
		for _, tmpFile := range tmpFiles {
			movableFiles <- tmpFile
		}
	}()

	go processInotify(watcher, writesFromInotify)
	go checkIfMovable(tmpDir, writesFromInotify, movableFiles)
	go handleMovableFiles(tmpDir, outDir, movableFiles, newFileMeta)
}

func main() {
	log.Printf("Hello, world! Watching: %s and moving to %s", TAGZ_IN_DIR, TAGZ_OUT_DIR)
	newFileMeta := make(chan FileMeta)
	watcher := makeWatcher(TAGZ_IN_DIR)
	defer watcher.Close()

	watchForNewFiles(watcher, TAGZ_TMP_DIR, TAGZ_OUT_DIR, newFileMeta)
	for fm := range newFileMeta {
		log.Printf("New file: %s %d %s %s %x", fm.origName, fm.sizeB, fm.modTime, fm.mime, fm.sha256)
	}
	log.Print("Exiting!")
}
