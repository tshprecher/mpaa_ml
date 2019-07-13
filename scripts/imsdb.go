package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"golang.org/x/net/html"
	"net/http"
	"os"
	"strings"
	"sync"
)

const (
	imsdbScriptEndpoint = "https://www.imsdb.com/scripts/%s.html"
)

var (
	stdoutMutex sync.Mutex
)

// findAllNodes takes a root html.Node and a selector predicate and does a DFS
// over the parsed html, retruning a slice of html.Nodes that match the predicate.
func findAllNodes(root *html.Node, selector func(*html.Node) bool) []*html.Node {
	// TODO: properly handle the case where root has siblings
	if root == nil {
		return nil
	}
	nodes := make([]*html.Node, 0)
	if selector(root) {
		nodes = append(nodes, root)
	}
	for next := root.FirstChild; next != nil; next = next.NextSibling {
		childrenSelected := findAllNodes(next, selector)
		for _, nde := range childrenSelected {
			nodes = append(nodes, nde)
		}
	}
	return nodes
}

// scrapeScript scrapes the script html node from imsdb.com
func scrapeScript(title string) (node *html.Node, err error) {
	formattedTitle := strings.Replace(title, " ", "_", -1)
	endpoint := fmt.Sprintf(imsdbScriptEndpoint, formattedTitle)
	resp, err := http.Get(endpoint)
	if err != nil {
		return
	}
	if resp.StatusCode != 200 {
		err = fmt.Errorf("unexpected status code %d", resp.StatusCode)
		return
	}

	defer resp.Body.Close()
	root, err := html.Parse(resp.Body)
	if err != nil {
		return
	}

	scriptNodes := findAllNodes(root, func(n *html.Node) bool {
		if n.Type == html.ElementNode && n.Data == "pre" {
			if n.FirstChild != nil {
				// bad title still return a page with a <pre> block, so
				// filter out such blocks without any contents
				return true
			}
		}
		return false
	})

	if len(scriptNodes) != 1 {
		err = fmt.Errorf("expected 1 node when scraping script, found %d", len(scriptNodes))
		return
	}
	node = scriptNodes[0]
	return
}

// parseContents traverses the root html node and dumps the raw
// contents parsing out html blocks
func parseContents(root *html.Node, contents *bytes.Buffer) {
	if root.Type != html.ElementNode {
		contents.WriteString(root.Data)
	}
	for next := root.FirstChild; next != nil; next = next.NextSibling {
		parseContents(next, contents)
	}
}

func printScrapeFailure(title, typ, msg string) {
	stdoutMutex.Lock()
	defer stdoutMutex.Unlock()

	if typ != "" {
		fmt.Printf("failure:\t%s\t%s:%s\n", title, typ, msg)
	} else {
		fmt.Printf("failure:\t%s\t%s\n", title, msg)
	}
}

func printScrapeSuccess(title string) {
	stdoutMutex.Lock()
	defer stdoutMutex.Unlock()
	fmt.Printf("success:\t%s\n", title)
}

func main() {
	flag.Parse()

	inputReader := bufio.NewReader(os.Stdin)
	sch := make(chan string, 1000)

	// spin up some goroutines that read and fetch scripts
	var wg sync.WaitGroup
	ng := 100
	for i := 0; i < ng; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range sch {
				terms := strings.Split(line, ",")
				if len(terms) != 3 {
					printScrapeFailure("[unknown]", "", "invalid input line")
					continue
				}
				title := strings.TrimSpace(terms[0])
				filename := strings.Replace(strings.ToLower(title), " ", "_", -1)
				filenameTxt := filename + ".txt"
				filenameMeta := filename + ".meta"

				_, err := os.Open(filenameMeta)
				if err == nil {
					printScrapeFailure(title, "", "script already found")
					continue
				}
				scriptNode, err := scrapeScript(title)
				if err != nil {
					printScrapeFailure(title, "scrape error", err.Error())
					continue
				} else {
					buf := &bytes.Buffer{}
					parseContents(scriptNode, buf)

					// write the script contents					
					
					fileTxt, err := os.Create(filenameTxt)
					defer fileTxt.Close()
					if err != nil {
						printScrapeFailure(title, "file txt open", err.Error())
						continue
					}

					_, err = fileTxt.Write(buf.Bytes())
					if err != nil {
						printScrapeFailure(title, "file txt write", err.Error())
						continue
					}

					// write the script metadata for associating the content rating
					
					fileMeta, err := os.Create(filenameMeta)
					defer fileMeta.Close()
					if err != nil {
						printScrapeFailure(title, "file meta open", err.Error())
						continue
					}

					_, err = fileMeta.WriteString(line+"\n")
					if err != nil {
						printScrapeFailure(title, "file meta write", err.Error())
						continue
					}

					printScrapeSuccess(title)
				}
			}
		}()
	}

	for line, _, _ := inputReader.ReadLine(); len(line) != 0; line, _, _ = inputReader.ReadLine() {
		sch <- string(line)
	}
	close(sch)

	wg.Wait()
}
