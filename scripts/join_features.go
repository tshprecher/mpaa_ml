package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
)

var (
	inputDir = flag.String("in", "", "input directory to read feature csvs")
)

// exitError takes and error, prints it, and exits with a non-zero return code.
func exitError(err error) {
	fmt.Printf("error: %v\n", err)
	os.Exit(1)
}

type movie struct {
	title         string
	contentRating string
	features      map[string]int
}

// outerJoinMovies takes a slice of movies and does an outer join of the movies
// across the entire feature set. It returns a csv file with the features as columns.
func outerJoinMovies(movies []movie, featureSet map[string]struct{}, pctMin, pctMax int) string {
	// remove all features that occur in less than pctMin and greater than pctMax percent of the films
	featureOccurrences := map[string]int{}
	for f := range featureSet {
		featureOccurrences[f] = 0
		//fmt.Printf("DEBUG: filtering occurrence for feature %s\n", f)
		for _, m := range movies {
			if c := m.features[f]; c > 0 {
				featureOccurrences[f]++
			}
		}
	}
	for f, c := range featureOccurrences {
		if (c * 100 < len(movies)*pctMin) || (c * 100 > len(movies)*pctMax) {
			delete(featureSet, f)
		}
	}

	buf := &bytes.Buffer{}
	featuresSorted := []string{}
	for f := range featureSet {
		featuresSorted = append(featuresSorted, f)
	}
	sort.Strings(featuresSorted)

	// write the feature columns first
	buf.WriteString(fmt.Sprintf("title,content_rating,%s\n", strings.Join(featuresSorted, ",")))

	for _, m := range movies {
		// always write the tile and content type
		buf.WriteString(fmt.Sprintf("%s,%s", m.title, m.contentRating))

		for _, f := range featuresSorted {
			buf.WriteString(fmt.Sprintf(",%d", m.features[f]))
		}
		buf.WriteString("\n")
	}
	return buf.String()

}

// Read from a directory of feature-{title}.csv files and do an outer join on them
// so that the number of records is equal to the number of movies and all terms across
// all movies are included in the feature set. Movies where the term the does exist are
// automatically filled with zeros. The "content_rating" feature is the only one required
// for all input, and it is explicitly set as the first feature. The rest of the features
// are the contents terms sorted in lexicographic order.
//
// This script only exists because DataFrame.merge() has proved to be slow on a local machine
// with the number of features per each movie.
func main() {
	flag.Parse()

	if *inputDir == "" {
		exitError(errors.New("input directory required\n"))
	}

	files, err := ioutil.ReadDir(*inputDir)
	if err != nil {
		exitError(err)
	}

	featureSet := map[string]struct{}{}
	movies := []movie{}

	for _, f := range files {
		if !f.IsDir() {
			title := strings.Replace(strings.Replace(f.Name(), "features-", "", -1), ".csv", "", -1)

			file, err := os.Open(*inputDir + "/" + f.Name())
			if err != nil {
				exitError(errors.New("could not open file " + f.Name()))
			}

			br := bufio.NewReader(file)
			line, _ := br.ReadString('\n')
			features := strings.Split(line, ",")
			line, _ = br.ReadString('\n')
			featureValues := strings.Split(line, ",")

			file.Close()

			// skip content_rating
			if features[0] != "content_rating" {
				exitError(errors.New("first feature must be 'content_rating'"))
			}

			m := movie{title, featureValues[0], map[string]int{}}
			for i := range features[1:] {
				count, _ := strconv.Atoi(featureValues[i+1])
				featureSet[features[i+1]] = struct{}{}			
				m.features[features[i+1]] = count
			}

			movies = append(movies, m)
		}
	}
	fmt.Print(outerJoinMovies(movies, featureSet, 5, 90))
}
