package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"strings"
)

var (
	inputDir  = flag.String("in", "", "input directory to read .meta and .txt files")
	normRegex = regexp.MustCompile("[^a-zA-Z0-9]+")
)

// normalizeWord normalizes the words for feature extraction. All words are converted
// to lowercase and punctuation is eliminated
func normalizeWord(word string) string {
	word = strings.ToLower(word)
	return normRegex.ReplaceAllString(word, "")
}

// generateFeatures takes a title and creates a file `features-{title}.csv`. The first
// column (the target class) is the content rating. The rest are every single word
// and bigram normalized with the number of occurrences in the script.
func generateFeatures(title string) error {
	base := fmt.Sprintf("%s/%s", *inputDir, title)

	metaBytes, err := ioutil.ReadFile(base + ".meta")
	if err != nil {
		return err
	}

	textBytes, err := ioutil.ReadFile(base + ".txt")
	if err != nil {
		return err
	}

	metaTerms := strings.Split(string(metaBytes), ",")
	_ = metaTerms
	textWords := strings.Fields(string(textBytes))

	featureCounts := map[string]int{}
	var lastWord, currentWord string

	for _, word := range textWords {
		word = normalizeWord(word)
		if word == "" {
			continue
		}
		// increment the single word
		if _, ok := featureCounts[word]; ok {
			featureCounts[word]++
		} else {
			featureCounts[word] = 1
		}

		// increment the bigram
		lastWord = currentWord
		currentWord = word
		if lastWord == "" || currentWord == "" {
			continue
		}

		bigram := lastWord + "_" + currentWord
		if _, ok := featureCounts[bigram]; ok {
			featureCounts[bigram]++
		} else {
			featureCounts[bigram] = 1
		}
	}

	// sorting is not technically required here, but it's nice
	features := []string{}
	for f, _ := range featureCounts {
		features = append(features, f)
	}
	sort.Strings(features)

	featFile, err := os.Create(fmt.Sprintf("features-%s.csv", title))
	if err != nil {
		return err
	}
	defer featFile.Close()

	// write feature names
	featFile.WriteString("content_rating,")
	featFile.WriteString(strings.Join(features, ","))
	featFile.WriteString("\n")

	// write feature values
	featFile.WriteString(metaTerms[1])
	for _, f := range features {
		featFile.WriteString(",")
		featFile.WriteString(fmt.Sprintf("%d", featureCounts[f]))
	}
	featFile.WriteString("\n")

	//	fmt.Printf("DEBUG: feature counts for '%s': %d\n", title, len(features))
	return nil
}

// exitError takes and error, prints it, and exits with a non-zero return code.
func exitError(err error) {
	fmt.Printf("error: %v\n", err)
	os.Exit(1)
}

func main() {
	flag.Parse()

	if *inputDir == "" {
		exitError(errors.New("input directory required\n"))
	}

	files, err := ioutil.ReadDir(*inputDir)
	if err != nil {
		exitError(err)
	}

	uniqueTitles := map[string]struct{}{}
	for _, f := range files {
		if !f.IsDir() {
			uniqueTitle := strings.Replace(strings.Replace(f.Name(), ".meta", "", -1), ".txt", "", -1)
			uniqueTitles[uniqueTitle] = struct{}{}
		}
	}
	sortedTitles := []string{}
	for ut, _ := range uniqueTitles {
		sortedTitles = append(sortedTitles, ut)
	}
	sort.Strings(sortedTitles)

	for _, s := range sortedTitles {
		fmt.Printf("DEBUG: title -> '%s'\n", s)
		err := generateFeatures(s)
		if err != nil {
			exitError(err)
		}
	}
}
