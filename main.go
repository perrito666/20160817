package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"

	"golang.org/x/net/html"
)

const (
	redisAddr       = "127.0.0.1:6379"
	downloadedQueue = "zips"
	processedQueue  = "xmls"
)

const (
	feed           = ""
	hrefAttr       = "href"
	zipSuffix      = ".zip"
	zipConcurrency = 3
)

var minProtocolLen = len("http://")

func main() {
	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", redisAddr)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
	links := make(chan string)
	fail := make(chan error)
	done := make(chan struct{})
	for i := 0; i < zipConcurrency; i++ {
		go processLinks(links, fail, pool)
	}
	go downloadLinksList(feed, links, fail, done)
	select {
	case err := <-fail:
		log.Fatal(err)
	case <-done:
		return
	}
}

// processZip opens a zipfile in the given path and logs its conents a
// list in the given redis connection.
func processZip(zipFile, zipName string, c redis.Conn) error {
	log.Printf("Processing zip %s", zipName)
	r, err := zip.OpenReader(zipFile)
	if err != nil {
		return fmt.Errorf("cannot open zip file %q %v", zipFile, err)
	}
	defer r.Close()

	for _, f := range r.File {
		log.Printf("Processing xml %s", f.Name)
		reply, err := redis.String(c.Do("HGET", processedQueue, f.Name))
		if err != nil && err != redis.ErrNil {
			return fmt.Errorf("cannot check if xml exists: %v", err)
		}
		if len(reply) > 0 {
			continue
		}
		fd, err := f.Open()
		if err != nil {
			return fmt.Errorf("cannot open xml on zip: %v", err)
		}
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		_, err = io.Copy(writer, fd)
		if err != nil {
			return fmt.Errorf("cannot read xml in xip: %v", err)
		}
		writer.Flush()
		fd.Close()
		c.Do("LPUSH", "NEWS_XML", buf.String())
		_, err = c.Do("HSET", processedQueue, f.Name, f.Name)
		if err != nil {
			return fmt.Errorf("cannot set processed Queue: %v", err)
		}
	}
	return nil
}

// processLinks obtains links from the given channel and downloads their
// contents for posterior process, sending any failure through the fail
// channel.
func processLinks(links chan string, fail chan error, pool *redis.Pool) {
	c := pool.Get()
	defer c.Close()
	for {
		link := <-links

		reply, err := redis.String(c.Do("HGET", downloadedQueue, link))
		if err != nil && err != redis.ErrNil {
			fail <- fmt.Errorf("cannot check download queue: %v", err)
		}
		if len(reply) > 0 {
			log.Printf("Zip %s/%s already processed", feed, link)
			continue
		}

		tempFile, err := ioutil.TempFile("", "zip")
		if err != nil {
			fail <- fmt.Errorf("cannot open tempfile to write zip: %v", err)
		}
		defer os.Remove(tempFile.Name())

		log.Printf("Downloading zip %s/%s", feed, link)
		response, err := http.Get(feed + "/" + link)
		defer response.Body.Close()

		// lets get the contents into a file, we dont know the size
		// and therefore are not sure if we can hold many of these
		// in memory.
		_, err = io.Copy(tempFile, response.Body)
		if err != nil {
			fail <- fmt.Errorf("cannot copy response body from zip file into temp file: %v", err)
		}

		if err := processZip(tempFile.Name(), link, c); err != nil {
			fail <- fmt.Errorf("while processing zip: %v", err)
		}

		_, err = c.Do("HSET", downloadedQueue, link, link)
		if err != nil {
			fail <- fmt.Errorf("cannot set downloaded queue: %v", err)
		}

		os.Remove(tempFile.Name())
	}
}

// extractLink obtains href from a list of attributes
func extractLink(attrs []html.Attribute) string {
	for _, attr := range attrs {
		if attr.Key == hrefAttr {
			return attr.Val
		}
	}
	return ""
}

// downloadLinksList extracts a list of links to zip files from the given
// url and feeds them to the passed links channel.
func downloadLinksList(url string, links chan string, fail chan error, done chan struct{}) {
	response, err := http.Get(url)
	if err != nil {
		fail <- fmt.Errorf("cannot process url: %v", err)
	}
	defer response.Body.Close()
	log.Println("Succesful connection")
	tokenizer := html.NewTokenizer(response.Body)
	log.Println("Start parsing")
	for {
		tokenType := tokenizer.Next()
		switch tokenType {
		case html.ErrorToken:
			done <- struct{}{}
		case html.StartTagToken:
			// gets the current token
			token := tokenizer.Token()
			if token.Data != "a" {
				continue
			}
			link := extractLink(token.Attr)
			if len(link) < minProtocolLen {
				continue
			}
			if strings.HasSuffix(link, zipSuffix) {
				links <- link
			}
		}
	}
}
